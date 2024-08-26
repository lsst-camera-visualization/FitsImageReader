package org.lsst.fits.imageio;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.LookupOp;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.imageio.stream.ImageInputStream;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;
import nom.tam.fits.TruncatedFileException;
import nom.tam.util.BufferedFile;
import org.lsst.fits.imageio.bias.BiasCorrection;
import org.lsst.fits.imageio.bias.BiasCorrection.CorrectionFactors;
import org.lsst.fits.imageio.bias.NullBiasCorrection;
import org.lsst.fits.imageio.cmap.RGBColorMap;

/**
 * This is the main component of the camera image reader. It makes extensive use
 * of Caffeine caching library to deal with thread safety and performance issues
 * when several clients are attempting to read overlapping segments of a single
 * image at the same time.
 *
 * @author tonyj
 */
public class CachingReader {

    private record SegmentCacheKey(String line, Character wcsLetter, Map<String, Map<String, Object>> wcsOverride) {}
    private final AsyncLoadingCache<SegmentCacheKey, List<Segment>> segmentCache;

    /**
     * Caches the rawdata for a segment. Rawdata is the pixel data as read from
     * disk
     */
    private final AsyncLoadingCache<Segment, RawData> rawDataCache;

    // Note: Using a long array as a hash key is probably a bad idea, since presambly it requires scanning all the 
    // values to compute the hash.
    private record SegmentBiasCorrectionAndCounts(Segment segment, BiasCorrection biasCorrection, long[] counts, RGBColorMap cmap) {}
    private final AsyncLoadingCache<SegmentBiasCorrectionAndCounts, BufferedImage> bufferedImageCache;

    private record SegmentListAndBiasCorrection(List<Segment> segments, BiasCorrection biasCorrection) {}
    private final AsyncLoadingCache<SegmentListAndBiasCorrection, long[]> globalScalingCache;

    private record SegmentAndBiasCorrection(Segment segment, BiasCorrection biasCorrection) {}
    private final AsyncLoadingCache<SegmentAndBiasCorrection, CorrectionFactors> biasCorrectionCache;

    /**
     * Caches the lines read from the ImageInputStream
     */
    private final LoadingCache<ImageInputStream, List<String>> linesCache;

    private static final Logger LOG = Logger.getLogger(CachingReader.class.getName());

    public CachingReader() {

        segmentCache = Caffeine.newBuilder()
                .maximumSize(Integer.getInteger("org.lsst.fits.imageio.segmentCacheSize", 10))
                .recordStats()
                .buildAsync((SegmentCacheKey key) -> {
                    return Timed.execute(() -> {
                        return readSegment(key.line, key.wcsLetter, key.wcsOverride);
                    }, "Loading %s took %dms", key.line);
                });

        Weigher<Segment, RawData> rawDataWeigher = (Segment k1, RawData rawData) -> rawData.getBuffer().capacity() * 4;
        rawDataCache = Caffeine.newBuilder()
                .weigher(rawDataWeigher)
                .maximumWeight(Long.getLong("org.lsst.fits.imageio.rawDataCacheSizeBytes", 1_000_000L))
                .recordStats()
                .buildAsync((Segment segment, Executor executor) -> segment.readRawDataAsync(executor));

        biasCorrectionCache = Caffeine.newBuilder()
                .maximumSize(Integer.getInteger("org.lsst.fits.imageio.biasCorrectionCacheSize", 10))
                .recordStats()
                .buildAsync((SegmentAndBiasCorrection key, Executor executor) -> {
                    Segment segment = key.segment;
                    return rawDataCache.get(segment).thenApply(rawData -> {
                        if (rawData.getBuffer() instanceof IntBuffer intBuffer) {
                            return key.biasCorrection.compute(intBuffer, segment);
                        } else {
                            return new NullBiasCorrection().compute(null, segment);
                        }
                    });
                });

        Weigher<SegmentBiasCorrectionAndCounts, BufferedImage> buffedImageWeigher = (SegmentBiasCorrectionAndCounts k1, BufferedImage bi) -> bi.getHeight() * bi.getWidth() * 4;
        bufferedImageCache = Caffeine.newBuilder()
                .weigher(buffedImageWeigher)
                .maximumWeight(Long.getLong("org.lsst.fits.imageio.bufferedImageCacheSizeBytes", 1_000_000L))
                .recordStats()
                .buildAsync((SegmentBiasCorrectionAndCounts key, Executor executor) -> {
                    return rawDataCache.get(key.segment).thenApply(rawData -> {
                        return biasCorrectionCache.get(new SegmentAndBiasCorrection(key.segment, key.biasCorrection)).thenApply(factors -> {
                            return Timed.execute(() -> {
                                if (rawData.getBuffer() instanceof IntBuffer) {
                                    return createBufferedImage((RawData<IntBuffer>) rawData, factors, key.counts, key.cmap);
                                } else {
                                    return createBufferedImage((RawData<FloatBuffer>) rawData);
                                }
                            }, "Loading buffered image for segment %s took %dms", key.segment);
                        }).join();
                    });
                });

        globalScalingCache = Caffeine.newBuilder()
                .maximumSize(Integer.getInteger("org.lsst.fits.imageio.globalScalingCacheSize", 10))
                .recordStats()
                .buildAsync((SegmentListAndBiasCorrection key, Executor executor) -> {
                    LOG.log(Level.FINE, "Building global scale for {0} {1} {2}", new Object[]{key.hashCode(), key.segments.hashCode(), key.biasCorrection.hashCode()});
                    List<CompletableFuture<ScalingUtils>> histograms = new ArrayList<>();
                    for (Segment segment : key.segments) {
                        histograms.add(rawDataCache.get(segment).thenApply((rawData) -> {
                            return biasCorrectionCache.get(new SegmentAndBiasCorrection(segment, key.biasCorrection)).thenApply(correctionFactors -> {
                                IntBuffer intData = (IntBuffer) rawData.getBuffer();
                                return histogram(segment.getDataSec(), intData, segment, correctionFactors);
                            }).join(); // Not clear doing a join inside the loop is optimal
                        }));
                    }
                    return CompletableFuture.allOf(histograms.toArray(CompletableFuture[]::new)).thenApply((v) -> {
                        try {
                            long[] counts = new long[1 << 18];
                            for (CompletableFuture<ScalingUtils> future : histograms) {
                                ScalingUtils su = future.get();
                                LOG.log(Level.FINE, "Adding bins with max {0}", su.getHighestOccupiedBin());
                                for (int i = su.getLowestOccupiedBin(); i <= su.getHighestOccupiedBin(); i++) {
                                    counts[i] += su.getCount(i);
                                }
                            }
                            return counts;
                        } catch (ExecutionException | InterruptedException x) {
                            throw new RuntimeException("Error computing global scale", x);
                        }
                    });

                });

        linesCache = Caffeine.newBuilder()
                .maximumSize(Integer.getInteger("org.lsst.fits.imageio.linesCacheSize", 10))
                .build((ImageInputStream in) -> {
                    return Timed.execute(() -> {
                        List<String> lines = new ArrayList<>();
                        in.seek(0);
                        for (;;) {
                            String line = in.readLine();
                            if (line == null) {
                                break;
                            } else if (line.startsWith("#")) {
                                //continue;
                            } else {
                                lines.add(line);
                            }
                        }
                        return lines;
                    }, "Read lines in %dms");
                });
        // Report stats every minute
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                report();
            }
        }, 60_000, 60_000);
    }

    void report() {
        LoadingCache<SegmentCacheKey, List<Segment>> s1 = segmentCache.synchronous();
        //LOG.log(Level.INFO, "Skipping color map 2");
        LOG.log(Level.INFO, "segment Cache size {0} stats {1}", new Object[]{s1.estimatedSize(), s1.stats()});
        LoadingCache<Segment, RawData> s2 = rawDataCache.synchronous();
        LOG.log(Level.INFO, "rawData Cache size {0} stats {1}", new Object[]{s2.estimatedSize(), s2.stats()});
        LoadingCache<SegmentBiasCorrectionAndCounts, BufferedImage> s3 = bufferedImageCache.synchronous();
        LOG.log(Level.INFO, "bufferedImage Cache size {0} stats {1}", new Object[]{s3.estimatedSize(), s3.stats()});
        LoadingCache<SegmentListAndBiasCorrection, long[]> s4 = globalScalingCache.synchronous();
        LOG.log(Level.INFO, "globalScaling Cache size {0} stats {1}", new Object[]{s4.estimatedSize(), s4.stats()});
        LoadingCache<SegmentAndBiasCorrection, CorrectionFactors> s5 = biasCorrectionCache.synchronous();
        LOG.log(Level.INFO, "biasCorrection Cache size {0} stats {1}", new Object[]{s5.estimatedSize(), s5.stats()});
    }

    int preReadImage(ImageInputStream fileInput) {
        List<String> lines = linesCache.get(fileInput);
        return lines == null ? 0 : lines.size();
    }

    void readImage(ImageInputStream fileInput, Rectangle sourceRegion, Graphics2D g, RGBColorMap cmap, BiasCorrection bc, boolean showBiasRegion, char wcsLetter, long[] globalScale, Map<String, Map<String, Object>> wcsOverride) throws IOException {
        try {
            Queue<CompletableFuture<Void>> segmentsCompletables = new ConcurrentLinkedQueue<>();
            Queue<CompletableFuture<Void>> bufferedImageCompletables = new ConcurrentLinkedQueue<>();
            List<String> lines = linesCache.get(fileInput);
            lines.stream().map((line) -> segmentCache.get(new SegmentCacheKey(line, wcsLetter, wcsOverride))).forEach((CompletableFuture<List<Segment>> futureSegments) -> {
                segmentsCompletables.add(futureSegments.thenAccept((List<Segment> segments) -> {
                    List<Segment> segmentsToRead = computeSegmentsToRead(segments, sourceRegion);
                    segmentsToRead.stream().forEach((Segment segment) -> {
                        CompletableFuture<BufferedImage> fbi = bufferedImageCache.get(new SegmentBiasCorrectionAndCounts(segment, bc, globalScale, cmap));
                        bufferedImageCompletables.add(fbi.thenAccept((BufferedImage bi) -> {
                            Timed.execute(() -> {
                                // g2=g is the graphics we are writing into
                                Graphics2D g2 = (Graphics2D) g.create();
                                g2.transform(segment.getWCSTranslation(showBiasRegion));
                                BufferedImage subimage;
                                if (showBiasRegion) {
                                    subimage = bi;
                                } else {
                                    Rectangle datasec = segment.getDataSec();
                                    subimage = bi.getSubimage(datasec.x, datasec.y, datasec.width, datasec.height);
                                }
                                if (cmap != CameraImageReader.DEFAULT_COLOR_MAP && cmap != CameraImageReader.NULL_COLOR_MAP) {
                                    LookupOp op = cmap.getLookupOp();
                                    subimage = op.filter(subimage, null);
                                }
                                g2.drawImage(subimage, 0, 0, null);
                                g2.dispose();
                                return null;
                            }, "drawImage for segment %s took %dms", segment);
                        }));
                    });
                }));
            });
            LOG.log(Level.INFO, "Waiting for {0} files", segmentsCompletables.size());
            CompletableFuture.allOf(segmentsCompletables.toArray(CompletableFuture[]::new)).join();
            LOG.log(Level.INFO, "Waiting for {0} buffered images", bufferedImageCompletables.size());
            CompletableFuture.allOf(bufferedImageCompletables.toArray(CompletableFuture[]::new)).join();
            LOG.log(Level.INFO, "Done waiting");
        } catch (CompletionException x) {
            Throwable cause = x.getCause();
            if (cause instanceof IOException iOException) {
                throw iOException;
            } else {
                throw new IOException("Unexpected exception during image reading", cause);
            }
        }
    }

    void readImageWithOnTheFlyGlobalScale(ImageInputStream fileInput, Rectangle sourceRegion, Graphics2D g, RGBColorMap cmap, BiasCorrection bc, boolean showBiasRegion, char wcsLetter, Map<String, Map<String, Object>> wcsOverride) throws IOException {

        try {
            Queue<CompletableFuture<Void>> segmentsCompletables = new ConcurrentLinkedQueue<>();
            Queue<CompletableFuture<Void>> bufferedImageCompletables = new ConcurrentLinkedQueue<>();
            Queue<CompletableFuture<Void>> globalScaleCompletable = new ConcurrentLinkedQueue<>();
            List<String> lines = linesCache.get(fileInput);
            List<Segment> allSegments = new ArrayList<>();
            lines.stream().map((line) -> segmentCache.get(new SegmentCacheKey(line, wcsLetter, wcsOverride))).forEach((CompletableFuture<List<Segment>> futureSegments) -> {
                segmentsCompletables.add(futureSegments.thenAccept((List<Segment> segments) -> {
                    allSegments.addAll(segments);
                }));
            });
            LOG.log(Level.INFO, "Waiting for {0} files", segmentsCompletables.size());
            CompletableFuture.allOf(segmentsCompletables.toArray(CompletableFuture[]::new)).join();

            globalScaleCompletable.add(globalScalingCache.get(new SegmentListAndBiasCorrection(allSegments, bc)).thenAccept((long[] globalScale) -> {
                List<Segment> segmentsToRead = computeSegmentsToRead(allSegments, sourceRegion);
                segmentsToRead.stream().forEach((Segment segment) -> {
                    CompletableFuture<BufferedImage> fbi = bufferedImageCache.get(new SegmentBiasCorrectionAndCounts(segment, bc, globalScale, cmap));
                    bufferedImageCompletables.add(fbi.thenAccept((BufferedImage bi) -> {
                        Timed.execute(() -> {
                            // g2=g is the graphics we are writing into
                            Graphics2D g2 = (Graphics2D) g.create();
                            g2.transform(segment.getWCSTranslation(showBiasRegion));
                            BufferedImage subimage;
                            if (showBiasRegion) {
                                subimage = bi;
                            } else {
                                Rectangle datasec = segment.getDataSec();
                                subimage = bi.getSubimage(datasec.x, datasec.y, datasec.width, datasec.height);
                            }
                            if (cmap != CameraImageReader.DEFAULT_COLOR_MAP && cmap != CameraImageReader.NULL_COLOR_MAP) {
                                LookupOp op = cmap.getLookupOp();
                                subimage = op.filter(subimage, null);
                            }
                            g2.drawImage(subimage, 0, 0, null);
                            g2.dispose();
                            return null;
                        }, "drawImage for segment %s took %dms", segment);
                    }));
                });
            }));

            LOG.log(Level.INFO, "Waiting for {0} global scales", globalScaleCompletable.size());
            CompletableFuture.allOf(globalScaleCompletable.toArray(CompletableFuture[]::new)).join();
            LOG.log(Level.INFO, "Waiting for {0} buffered images", bufferedImageCompletables.size());
            CompletableFuture.allOf(bufferedImageCompletables.toArray(CompletableFuture[]::new)).join();
            LOG.log(Level.INFO, "Done waiting");
        } catch (CompletionException x) {
            Throwable cause = x.getCause();
            if (cause instanceof IOException iOException) {
                throw iOException;
            } else {
                throw new IOException("Unexpected exception during image reading", cause);
            }
        }
    }

    private List<Segment> computeSegmentsToRead(List<Segment> segments, Rectangle sourceRegion) {
        if (sourceRegion == null) {
            return segments;
        } else {
            return segments.stream()
                    .filter((segment) -> (segment.intersects(sourceRegion)))
                    .collect(Collectors.toCollection(ArrayList::new));
        }
    }

    /**
     * Read a segment either directly from the DAQ or from a FITS file
     *
     * @param line
     * @param wcsLetter
     * @param wcsOverride
     * @return The list of segments read
     * @throws IOException
     * @throws TruncatedFileException
     * @throws FitsException
     */
    private static List<Segment> readSegment(String line, char wcsLetter, Map<String, Map<String, Object>> wcsOverride) throws IOException, TruncatedFileException, FitsException {
        if (line.startsWith("DAQ:")) {
            return readDAQSegment(line, wcsLetter, wcsOverride);
        } else {
            return readFitsFileSegment(new File(line), wcsLetter, wcsOverride);
        }
    }

    private static List<Segment> readDAQSegment(String line, char wcsLetter, Map<String, Map<String, Object>> wcsOverride) throws IOException {
        // We can't read one segment from the DAQ, rather we have to read the raw data for an entire REB, and then extract the segments from that.
        // DAQ:camera:raw/MC_C_20210206_000109:R00/RebG
        Pattern pattern = Pattern.compile("DAQ:(\\w+):(\\w+)/(\\w+):(\\w+)/(\\w+)");
        Matcher matcher = pattern.matcher(line);
        if (!matcher.matches()) {
            throw new IOException("Illegal image segment descriptor: " + line);
        } else {
            String partition = matcher.group(1);
            String folder = matcher.group(2);
            String imageName = matcher.group(3);
            String raftName = matcher.group(4);
            String rebName = matcher.group(5);
            // Note, we don't actually need to read any data at this moment, we just need to return information about the amplifier location
            // Given focalplanegeometry, raftName, rebName, wcsLetter I would like to get back an  
            throw new IOException("Unsupported operation exception: " + line);
        }
    }

    private static List<Segment> readFitsFileSegment(File file, char wcsLetter, Map<String, Map<String, Object>> wcsOverride) throws IOException, TruncatedFileException, FitsException {
        List<Segment> result = new ArrayList<>();
        String ccdSlot = null;
        String raftBay = null;
        int nSegments = 16;
        boolean isDMFile = false;
        try ( BufferedFile bf = new BufferedFile(file, "r")) {
            for (int i = 0; i < nSegments + 1; i++) {
                Header header = new Header(bf);
                if (i == 0) {
                    raftBay = header.getStringValue("RAFTBAY");
                    ccdSlot = header.getStringValue("CCDSLOT");
                    long expId = header.getLongValue("EXPID");
                    if (ccdSlot == null) {
                        ccdSlot = header.getStringValue("SENSNAME");
                    }
                    if (ccdSlot == null) {
                        throw new IOException("Missing CCDSLOT while reading " + file);
                    }
                    if (expId != 0) { // Crude way to test if this is a DM file
                        nSegments = 1;
                        isDMFile = true;
                    } else if (ccdSlot.startsWith("SW")) {
                        nSegments = 8;
                    }
                    boolean isGuiderFile = header.containsKey("N_STAMPS");
                    if (isGuiderFile) {
                        LOG.log(Level.INFO, "skipping guider file {0}", file);
                        break;
                    }
                }
                if (i > 0) {
                    if (isDMFile) {
                        // This is correct for a single CCD (e.g. AuxTel)
                        // Will need more work for the general case
                        wcsLetter = 'D';
                        Map<String, Object> dmWCSOverride = new HashMap<>();
                        boolean isCompressed = header.getBooleanValue("ZIMAGE");
                        int naxis1, naxis2;
                        if (isCompressed) {
                            naxis1 = header.getIntValue("ZNAXIS1");
                            naxis2 = header.getIntValue("ZNAXIS2");
                        } else {
                            naxis1 = header.getIntValue("NAXIS1");
                            naxis2 = header.getIntValue("NAXIS2");
                        }
                        dmWCSOverride.put("DATASEC", String.format("[1:%d,1:%d]", naxis1, naxis2));
                        dmWCSOverride.put("PC1_1D", 1.0);
                        dmWCSOverride.put("PC1_2D", 0.0);
                        dmWCSOverride.put("PC2_1D", 0.0);
                        dmWCSOverride.put("PC2_2D", 1.0);
                        dmWCSOverride.put("CRVAL1D", 0);
                        dmWCSOverride.put("CRVAL2D", 0);
                        Segment segment = new Segment(header, file, bf, raftBay, ccdSlot, wcsLetter, dmWCSOverride);
                        result.add(segment);
                    } else {
                        String extName = header.getStringValue("EXTNAME");
                        String wcsKey = String.format("%s/%s/%s", raftBay, ccdSlot, extName.substring(7, 9));
                        Segment segment = new Segment(header, file, bf, raftBay, ccdSlot, wcsLetter, wcsOverride == null ? null : wcsOverride.get(wcsKey));
                        result.add(segment);
                    }
                }
            }
        }
        return result;
    }

    private static BufferedImage createBufferedImage(RawData<FloatBuffer> rawData) {
        FloatBuffer floatBuffer = rawData.getBuffer();

        EnhancedScalingUtils esu = new EnhancedScalingUtils(floatBuffer, CameraImageReader.DEFAULT_COLOR_MAP);
        Segment segment = rawData.getSegment();
        Rectangle datasec = segment.getDataSec();

        BufferedImage image = CameraImageReader.IMAGE_TYPE.createBufferedImage(segment.getNAxis1(), segment.getNAxis2());
        WritableRaster raster = image.getRaster();
        DataBuffer db = raster.getDataBuffer();

        for (int y = datasec.y; y < datasec.height + datasec.y; y++) {
            int p = datasec.x + y * segment.getNAxis1();
            for (int x = datasec.x; x < datasec.width + datasec.x; x++) {
                float f = floatBuffer.get(p);
                db.setElem(p, esu.getRGB(f));
                p++;
            }
        }
        return image;
    }

    private static BufferedImage createBufferedImage(RawData<IntBuffer> rawData, CorrectionFactors factors, long[] globalScale, RGBColorMap cmap) {
        IntBuffer intBuffer = rawData.getBuffer();
        Segment segment = rawData.getSegment();
        Rectangle datasec = segment.getDataSec();
        // Apply bias correction
        ScalingUtils su;
        if (globalScale != null) {
            su = new ScalingUtils(globalScale);
            LOG.log(Level.FINE, "Global scale max {0}", su.getHighestOccupiedBin());
        } else {
            su = histogram(datasec, intBuffer, segment, factors);
        }
        final int max = su.getHighestOccupiedBin();
        int[] cdf = su.computeCDF();

        int range = cdf[max];
        range = 1 + range / 256;
        for (int i = su.getLowestOccupiedBin(); i <= max; i++) {
            cdf[i] = CameraImageReader.DEFAULT_COLOR_MAP.getRGB(cdf[i] / range);
        }

        // Scale data 
        BufferedImage image = CameraImageReader.IMAGE_TYPE.createBufferedImage(segment.getNAxis1(), segment.getNAxis2());
        WritableRaster raster = image.getRaster();
        DataBuffer db = raster.getDataBuffer();
//        Used for testing bias region
//        Graphics2D graphics = image.createGraphics();        
//        graphics.setColor(Color.GREEN);
//        graphics.fillRect(0, 0, datasec.x, segment.getNAxis2());
//        graphics.setColor(Color.RED);
//        graphics.fillRect(datasec.x + datasec.width, 0, segment.getNAxis1() - datasec.x - datasec.width, segment.getNAxis2());
//        graphics.setColor(Color.BLUE);
//        graphics.fillRect(datasec.x, datasec.y + datasec.height, datasec.width, segment.getNAxis2());
        copyAndScaleData(datasec, segment, cdf, intBuffer, factors, db, max, cmap);
        return image;
    }

    private static void copyAndScaleData(Rectangle datasec, Segment segment, int[] cdf, IntBuffer intBuffer, BiasCorrection.CorrectionFactors factors, DataBuffer db, int max, RGBColorMap cmap) {
        //LOG.log(Level.WARNING, "copyAndScaleData called 1.7.1");
        //if (cmap.equals(CameraImageReader.NULL_COLOR_MAP)) LOG.log(Level.INFO, "copyAndScaleData null color map");
        //else if (cmap.equals(CameraImageReader.DEFAULT_COLOR_MAP)) LOG.log(Level.INFO, "copyAndScaleData default color map");
        //else LOG.log(Level.INFO, "copyAndScaleData other color map");
        boolean no_map = cmap.equals(CameraImageReader.NULL_COLOR_MAP);
        for (int y = datasec.y; y < datasec.height + datasec.y; y++) {
            int p = datasec.x + y * segment.getNAxis1();
            for (int x = datasec.x; x < datasec.width + datasec.x; x++) {
                final int correctionFactor = factors.correctionFactor(x, y);
//                if (correctionFactor < 0) {
//                    LOG.log(Level.WARNING, "Negative correction factor for {0} {1} {2} {3}", new Object[]{segment, x, y, correctionFactor});
//                }
                final int bin = Math.max(intBuffer.get(p) - correctionFactor, 0);
//                if (bin > max) {
//                    LOG.log(Level.WARNING, "Bin greater than max {0} {1} {2} {3} {4}", new Object[]{segment, x, y, bin, max});                    
//                }
                int rgb = no_map ? bin << 6 : cdf[bin]; 
                db.setElem(p, rgb);
                p++;
            }
        }
    }

    /**
     * Compute a histogram for the specified segment
     *
     * @param datasec The datasec to use to extract data
     * @param intBuffer The buffer containing the data
     * @param segment The segment
     * @param factors The bias correction to apply prior to histogramming
     * @return The ScalingUtils object built from the histogram
     */
    private static ScalingUtils histogram(Rectangle datasec, IntBuffer intBuffer, Segment segment, BiasCorrection.CorrectionFactors factors) {
        // Note: This is hardwired for Camera (18 bit) integer data
        int[] count = new int[1 << 18];
        for (int y = datasec.y; y < datasec.height + datasec.y; y++) {
            int p = datasec.x + y * segment.getNAxis1();
            for (int x = datasec.x; x < datasec.width + datasec.x; x++) {
                count[Math.max(intBuffer.get(p) - factors.correctionFactor(x, y), 0)]++;
                p++;
            }
        }
        return new ScalingUtils(count);
    }

    public List<Segment> readSegments(ImageInputStream in, char wcsLetter) {
        List<Segment> result = new ArrayList<>();
        List<String> lines = linesCache.get(in);
        for (String line : lines) {
            result.addAll((List<Segment>) (segmentCache.get(new SegmentCacheKey(line, wcsLetter, null)).join()));
        }
        return result;
    }

    public RawData getRawData(Segment segment) {
        return rawDataCache.get(segment).join();
    }

    BufferedImage getBufferedImage(Segment segment, BiasCorrection bc, long[] globalScale, RGBColorMap cmap) {
        final SegmentBiasCorrectionAndCounts key = new SegmentBiasCorrectionAndCounts(segment, bc, globalScale, cmap);
        CompletableFuture<BufferedImage> fi = bufferedImageCache.get(key);
        return fi.join();
    }

    long[] getGlobalScale(ImageInputStream fileInput, BiasCorrection bc, char wcsLetter, Map<String, Map<String, Object>> wcsOverride) {
        Queue<CompletableFuture<Void>> segmentsCompletables = new ConcurrentLinkedQueue<>();
        List<String> lines = linesCache.get(fileInput);
        List<Segment> allSegments = new ArrayList<>();
        lines.stream().map((line) -> segmentCache.get(new SegmentCacheKey(line, wcsLetter, wcsOverride))).forEach((CompletableFuture<List<Segment>> futureSegments) -> {
            segmentsCompletables.add(futureSegments.thenAccept((List<Segment> segments) -> {
                allSegments.addAll(segments);
            }));
        });
        CompletableFuture.allOf(segmentsCompletables.toArray(CompletableFuture[]::new)).join();

        return globalScalingCache.get(new SegmentListAndBiasCorrection(allSegments, bc)).join();
    }

    CorrectionFactors getCorrectionFactors(Segment segment, BiasCorrection bc) {
        return biasCorrectionCache.get(new SegmentAndBiasCorrection(segment, bc)).join();
    }
}
