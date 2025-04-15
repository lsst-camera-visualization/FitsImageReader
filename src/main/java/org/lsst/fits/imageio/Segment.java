package org.lsst.fits.imageio;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import nom.tam.fits.FitsException;
import nom.tam.fits.FitsUtil;
import nom.tam.fits.Header;
import nom.tam.fits.compression.algorithm.api.ICompressor;
import nom.tam.fits.compression.algorithm.gzip2.GZip2Compressor;
import nom.tam.fits.compression.algorithm.rice.RiceCompressOption;
import nom.tam.fits.compression.algorithm.rice.RiceCompressor.IntRiceCompressor;
import nom.tam.fits.header.Standard;
import nom.tam.util.BufferedFile;

/**
 * Represents one segment (amplifier) of a FITS file
 *
 * @author tonyj
 */
public class Segment {

    private static final Pattern DATASET_PATTERN = Pattern.compile("\\[(\\d+):(\\d+),(\\d+):(\\d+)\\]");
    private static final Logger LOG = Logger.getLogger(Segment.class.getName());

    private final File file;
    private long seekPosition;
    private final Rectangle2D.Double wcs;
    private final AffineTransform wcsTranslation;
    private Rectangle datasec;
    private int nAxis1;
    private int nAxis2;
    private double crval1;
    private double crval2;
    private double pc1_1;
    private double pc2_2;
    private double pc1_2;
    private double pc2_1;
    private final char wcsLetter;
    private int rawDataLength;
    private boolean isCompressed;
    private int cAxis1;
    private int cAxis2;
    private final String segmentName;
    private final String raftBay;
    private final String ccdSlot;
    private String compressionType;
    private int bitpix;
    private long fileSize;
    private final int hduNumber;

    public Segment(Header header, int hduNumber, long fileSize, File file, BufferedFile bf, String raftBay, String ccdSlot, char wcsLetter, Map<String, Object> wcsOverride) throws IOException, FitsException {
        this.file = file;
        this.hduNumber = hduNumber;
        this.fileSize = fileSize;
        this.seekPosition = bf.getFilePointer();
        this.wcsLetter = wcsLetter;
        this.raftBay = raftBay;
        this.ccdSlot = ccdSlot;
        this.segmentName = header.getStringValue("EXTNAME");
        extractheaderInfo(header);
        // Skip the data (for now)
        int pad = FitsUtil.padding(rawDataLength);
        bf.skip(rawDataLength + pad);

        if (wcsOverride != null) {
            String datasecString = wcsOverride.get("DATASEC").toString();
            datasec = computeDatasec(datasecString);
            pc1_1 = ((Number) wcsOverride.get("PC1_1" + wcsLetter)).doubleValue();
            pc2_2 = ((Number) wcsOverride.get("PC2_2" + wcsLetter)).doubleValue();
            pc1_2 = ((Number) wcsOverride.get("PC1_2" + wcsLetter)).doubleValue();
            pc2_1 = ((Number) wcsOverride.get("PC2_1" + wcsLetter)).doubleValue();
            crval1 = ((Number) wcsOverride.get("CRVAL1" + wcsLetter)).doubleValue();
            crval2 = ((Number) wcsOverride.get("CRVAL2" + wcsLetter)).doubleValue();
        } else {
            String datasecString = header.getStringValue("DATASEC");
            if (datasecString == null) {
                throw new IOException("Missing datasec for file: " + file);
            }
            // Note, for corner rafts, we see that the Q (raft) coordinates are not correct,
            // so instead we use the E (focal-plane) coordinates, and shift based on the raft position.
            char localWcsLetter = wcsLetter;
            if (wcsLetter == 'Q' && raftBay != null) {
                localWcsLetter = 'E';
            }
            datasec = computeDatasec(datasecString);
            pc1_1 = header.getDoubleValue("PC1_1" + localWcsLetter);
            pc2_2 = header.getDoubleValue("PC2_2" + localWcsLetter);
            pc1_2 = header.getDoubleValue("PC1_2" + localWcsLetter);
            pc2_1 = header.getDoubleValue("PC2_1" + localWcsLetter);
            crval1 = header.getDoubleValue("CRVAL1" + localWcsLetter);
            crval2 = header.getDoubleValue("CRVAL2" + localWcsLetter);
            if (wcsLetter == 'Q' && raftBay != null) {
                int raftX = Integer.parseInt(raftBay.substring(1,2));
                int raftY = Integer.parseInt(raftBay.substring(2,3));
                crval1 -= raftY*12700;
                crval2 -= raftX*12700;
                //double oldCrval1 = header.getDoubleValue("CRVAL1" + "Q");
                //double oldCrval2 = header.getDoubleValue("CRVAL2" + "Q");
                //System.out.printf("File: %s Raft: %d %d crval1 %g crval2 %g oldcrval1 %g oldcrval2 %g\n", file, raftX, raftY, crval1, crval2, oldCrval1, oldCrval2);
            }
        }
        //This does not work for corner rafts!
        //ccdX = Integer.parseInt(ccdSlot.substring(1, 2));
        //ccdY = Integer.parseInt(ccdSlot.substring(2, 3));
        wcsTranslation = new AffineTransform(pc1_1, pc2_1, pc1_2, pc2_2, crval1, crval2);
        wcsTranslation.translate(datasec.x + 0.5, datasec.y + 0.5);
        //wcsTranslation.translate(crval1, crval2);
        //wcsTranslation.scale(pc1_1, pc2_2);
        //System.out.printf("FILE %s CCDSLOT %s\n", file, ccdSlot);
        //System.out.printf("pc1_1=%3.3g pc2_2=%3.3g pc1_2=%3.3g pc2_1=%3.3g\n", pc1_1, pc2_2, pc1_2, pc2_1);
        //System.out.printf("qcs=%s\n", wcsTranslation);
        Point2D origin = wcsTranslation.transform(new Point(0, 0), null);
        Point2D corner = wcsTranslation.transform(new Point(datasec.width, datasec.height), null);
        double x = Math.min(origin.getX(), corner.getX());
        double y = Math.min(origin.getY(), corner.getY());
        double width = Math.abs(origin.getX() - corner.getX());
        double height = Math.abs(origin.getY() - corner.getY());
        wcs = new Rectangle2D.Double(x, y, width, height);
    }

    private int extractheaderInfo(Header header) throws FitsException {
        this.isCompressed = header.getBooleanValue("ZIMAGE");
        if (isCompressed) {
            bitpix = header.getIntValue("ZBITPIX");
            compressionType = header.getStringValue("ZCMPTYPE");
            // Note, nom,tam.fits has support for many/all compression types,
            // but performance for the way we are trying to use it leaves much
            // to be desired. The "deferred data reading" used by nom.tam.fits also
            // has issues with requiring files to remain open to be used later.
            // For now we only support the GZIP_2 type used for camera data.
            if (!"RICE_1".equals(compressionType) && !"GZIP_2".equals(header.getStringValue("ZCMPTYPE"))) {
                throw new FitsException("Unsupported compression type: " + compressionType);
            }
            nAxis1 = header.getIntValue("ZNAXIS1"); // 576
            nAxis2 = header.getIntValue("ZNAXIS2"); // 2048
            rawDataLength = header.getIntValue(Standard.NAXIS1) * header.getIntValue(Standard.NAXIS2) + header.getIntValue("PCOUNT");
            // There give the size of the binary table giving the offsets into the compressed data
            cAxis1 = header.getIntValue(Standard.NAXIS1); // 8
            cAxis2 = header.getIntValue(Standard.NAXIS2); // 2048
            // These give the size of the compressed "tiles"
        } else {
            bitpix = header.getIntValue("BITPIX");
            nAxis1 = header.getIntValue(Standard.NAXIS1);
            nAxis2 = header.getIntValue(Standard.NAXIS2);
            rawDataLength = nAxis1 * nAxis2 * 4;
            cAxis1 = cAxis2 = 0;
            compressionType = null;
        }
        return rawDataLength;
    }

    private Rectangle computeDatasec(String datasecString) throws IOException, NumberFormatException {
        Matcher matcher = DATASET_PATTERN.matcher(datasecString);
        if (!matcher.matches()) {
            throw new IOException("Invalid datasec: " + datasecString);
        }
        int datasec1 = Integer.parseInt(matcher.group(1)) - 1;
        int datasec2 = Integer.parseInt(matcher.group(2));
        int datasec3 = Integer.parseInt(matcher.group(3)) - 1;
        int datasec4 = Integer.parseInt(matcher.group(4));
        return new Rectangle(datasec1, datasec3, datasec2 - datasec1, datasec4 - datasec3);
    }

    public int getImageSize() {
        return nAxis1 * nAxis2 * 4;
    }

    public int getDataSize() {
        return rawDataLength;
    }

    public File getFile() {
        return file;
    }

//    // Data in the compressed byte array is stored with the bytes shuffled, this routine unshuffles them
//    private void unshuffle(byte[] in, IntBuffer out) {
//        int length = in.length / 4;
//        for (int i = 0; i < length; i++) {
//            out.put(((0xff & in[i]) << 24) + ((0xff & in[i + length]) << 16) + ((0xff & in[i + 2 * length]) << 8) + ((0xff & in[i + 3 * length])));
//        }
//    }

//    Old method no longer used, left here in case there is a performance decrease with new version
//    private IntBuffer decodeGZIP2CompressedData(ByteBuffer bb) {
//        // We use inflater directly since it can be reused (unlike GZIPInputStream)
//        Inflater inflater = new Inflater(true);
//        try {
//            IntBuffer result = IntBuffer.allocate(nAxis1 * nAxis2);
//            // offsets contain the length and offset of each tile
//            int[] offsets = new int[cAxis1 * cAxis2 / 4];
//            bb.asIntBuffer().get(offsets);
//            bb.position(cAxis1 * cAxis2);
//            // Note, the +1 is required by Inflater (actually zlib)
//            byte[] decompressed = new byte[nAxis1 * 4 + 1];
//            // We assume the compressed data will never be larger than the decompressed
//            // data, but that might not be true.
//            byte[] compressed = new byte[nAxis1 * 4 + 1];
//            // Note, the format is designed to allow decompression to be parallelized
//            // but since we are typically reading many files in parallel there is little
//            // to be gained.
//            for (int i = 0; i < cAxis2; i++) {
//                // For jdk 8 we need to copy the input data into a byte array. If we
//                // use jdk11 we can directly send the ByteBuffer to the inflater.
//                bb.get(compressed, 0, offsets[i * 2]);
//                inflater.reset();
//                // 10 is to skip the gzip header
//                inflater.setInput(compressed, 10, offsets[i * 2] - 10);
//                int p = 0;
//                while (!inflater.finished()) {
//                    int l = inflater.inflate(decompressed, p, decompressed.length - p);
//                    p += l;
//                }
//                unshuffle(decompressed, result);
//            }
//            result.flip();
//            return result;
//        } catch (DataFormatException x) {
//            throw new CompletionException("Error decompressing image", x);
//        } finally {
//            inflater.end();
//        }
//    }
    
    private IntBuffer decodeGZIP2CompressedData(ByteBuffer bb) {
        return this.decodeCompressedData(bb, new GZip2Compressor.IntGZip2Compressor());
    }

    private FloatBuffer decodeGZIP2FloatCompressedData(ByteBuffer bb) {
        return this.decodeCompressedFloatData(bb, new GZip2Compressor.FloatGZip2Compressor());
    }
    
    private IntBuffer decodeRICECompressedData(ByteBuffer bb) {
        final RiceCompressOption riceCompressOption = new RiceCompressOption();
        // Why are these hardwired? -- presumably should come from headers.
        riceCompressOption.setBlockSize(32);
        riceCompressOption.setBytePix(4);
        IntRiceCompressor inflater = new IntRiceCompressor(riceCompressOption);
        return this.decodeCompressedData(bb, inflater);
    }
    
    // The compressed data is store as a FITS BinaryTable, where each row of the image is decompressed 
    // independently.
    private IntBuffer decodeCompressedData(ByteBuffer bb, ICompressor<IntBuffer> inflater) {
        IntBuffer result = IntBuffer.allocate(nAxis1 * nAxis2);
        // offsets contain the length and offset of each tile
        int[] offsets = new int[cAxis1 * cAxis2 / 4];
        bb.asIntBuffer().get(offsets);
        bb.position(cAxis1 * cAxis2);
        // Note, the format is designed to allow decompression to be parallelized
        // but since we are typically reading many files in parallel there is little
        // to be gained.
        for (int i = 0; i < cAxis2; i++) {
            result.limit(result.position()+nAxis1);
            bb.limit(bb.position()+offsets[i * 2]);
            inflater.decompress(bb, result.slice());
            result.position(result.position()+nAxis1);
        }
        result.flip();
        return result;
    }

    private FloatBuffer decodeCompressedFloatData(ByteBuffer bb, ICompressor<FloatBuffer> inflater) {
        FloatBuffer result = FloatBuffer.allocate(nAxis1 * nAxis2);
        // offsets contain the length and offset of each tile
        int[] offsets = new int[cAxis1 * cAxis2 / 4];
        bb.asIntBuffer().get(offsets);
        bb.position(cAxis1 * cAxis2);
        // Note, the format is designed to allow decompression to be parallelized
        // but since we are typically reading many files in parallel there is little
        // to be gained.
        for (int i = 0; i < cAxis2; i++) {
            result.limit(result.position()+nAxis1);
            bb.limit(bb.position()+offsets[i * 2]);
            inflater.decompress(bb, result.slice());
            result.position(result.position()+nAxis1);
        }
        result.flip();
        return result;
    }

    public CompletableFuture<RawData> readRawDataAsync(Executor executor) {

        CompletableFuture<ByteBuffer> futureByteBuffer = readByteBufferAsync();
        if (isCompressed) {
            if ("GZIP_2".equals(compressionType)) {
                switch (bitpix) {
                    case 32:
                        return futureByteBuffer.thenApply((bb) -> decodeGZIP2CompressedData(bb)).thenApply((ib) -> new RawData(this, ib));
                    case -32:
                        return futureByteBuffer.thenApply((bb) -> decodeGZIP2FloatCompressedData(bb)).thenApply((fb) -> new RawData(this, fb));
                    default:
                        throw new RuntimeException("Unsupported bitpix: "+bitpix);
                }
            } else {
                return futureByteBuffer.thenApply((bb) -> decodeRICECompressedData(bb)).thenApply((ib) -> new RawData(this, ib));
            }
        } else {
            switch (bitpix) {
                case 32:
                    return futureByteBuffer.thenApply((bb) -> new RawData(this, bb.asIntBuffer()));
                case -32:
                    return futureByteBuffer.thenApply((bb) -> new RawData(this, bb.asFloatBuffer()));
                default:
                    throw new RuntimeException("Unsupported bitpix: "+bitpix);
            }
        }
    }

    private CompletableFuture<ByteBuffer> readByteBufferAsync() {
        CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
        try {
            // This re-opens the file, which may have changed since we originally read it (e.g. it may be been compressed)
            // Any decision about whether this file is compressed or not, needs to be remade now. Also any offset we stored
            // into the full file may now be invalid.
            AsynchronousFileChannel asyncChannel = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ);  
            LOG.log(Level.INFO, "Checking file length modified {0} {1}", new Object[]{fileSize, file.length()});
            if (fileSize != file.length()) {
                LOG.log(Level.WARNING, "File {0} modified, taking evasive maneuvers", file);
                try (BufferedFile bf = new BufferedFile(file, "r")) {
                    @SuppressWarnings("UnusedAssignment")
                    Header header = new Header(bf); // Skip primary header
                    for (int i = 1; i < hduNumber; i++) {
                        header = new Header(bf);
                        bf.seek(bf.getFilePointer() + header.getDataSize());
                    }
                    this.fileSize  = file.length();
                    header = new Header(bf);
                    this.seekPosition = bf.getFilePointer();
                    // SanityCheck
                    if (!this.segmentName.equals(header.getStringValue("EXTNAME"))) {
                        throw new IOException("Segment name has changed, was "+this.segmentName+" now "+header.getStringValue("EXTNAME"));
                    }
                    extractheaderInfo(header);
                } 
            }
            ByteBuffer bb = ByteBuffer.allocateDirect(rawDataLength);
            bb.order(ByteOrder.BIG_ENDIAN);
            asyncChannel.read(bb, seekPosition, result, new CompletionHandler<Integer, CompletableFuture<ByteBuffer>>() {
                @Override
                public void completed(Integer len, CompletableFuture<ByteBuffer> future) {
                    try {
                        asyncChannel.close();
                    } catch (IOException ex) {
                        future.completeExceptionally(ex);
                    }
                    bb.flip();
                    future.complete(bb);
                }

                @Override
                public void failed(Throwable x, CompletableFuture<ByteBuffer> future) {
                    try {
                        asyncChannel.close();
                    } catch (IOException ex) {
                        // We already have an IO exception in progress, so ignore this additional one
                    }
                    future.completeExceptionally(x);
                }
            });
        } catch (IOException | FitsException x) {
            result.completeExceptionally(x);
        }
        return result;
    }

    public int getNAxis1() {
        return nAxis1;
    }

    public int getNAxis2() {
        return nAxis2;
    }

    public AffineTransform getWCSTranslation(boolean includeOverscan) {
        if (includeOverscan) {
//            int parallel_overscan = nAxis2 - datasec.height;
//            int serial_overscan = nAxis1 - datasec.width;
//            AffineTransform wcsTranslation = new AffineTransform();
//            int c = channel > 8 ? 16 - channel : channel - 1;
//            wcsTranslation.translate(crval1 + ccdY * serial_overscan * 8 + (c % 8) * serial_overscan, crval2 - parallel_overscan * pc2_2);
//            wcsTranslation.scale(pc1_1, pc2_2);
            return wcsTranslation;
        } else {
            return wcsTranslation;
        }
    }

    public Rectangle getDataSec() {
        return datasec;
    }

    boolean intersects(Rectangle sourceRegion) {
        return wcs.intersects(sourceRegion);
    }

    public Rectangle2D.Double getWcs() {
        return wcs;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public String getRaftBay() {
        return raftBay;
    }

    public String getCcdSlot() {
        return ccdSlot;
    }

    @Override
    public String toString() {
        return "Segment{" + "file=" + file + ", name=" + segmentName + ", raftBay=" + raftBay + ", ccdSlot=" + ccdSlot + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 71 * hash + Objects.hashCode(this.file);
        hash = 71 * hash + this.hduNumber;
        hash = 71 * hash + Objects.hashCode(this.wcsLetter);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Segment other = (Segment) obj;
        if (this.hduNumber != other.hduNumber) {
            return false;
        }
        if (!Objects.equals(this.wcsLetter, other.wcsLetter)) {
            return false;
        }
        return Objects.equals(this.file, other.file);
    }

}
