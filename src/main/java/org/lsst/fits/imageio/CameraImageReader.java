package org.lsst.fits.imageio;

import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;
import nom.tam.fits.FitsFactory;
import org.lsst.fits.imageio.bias.BiasCorrection;
import org.lsst.fits.imageio.bias.BiasCorrection.CorrectionFactors;
import org.lsst.fits.imageio.bias.NullBiasCorrection;
import org.lsst.fits.imageio.bias.SerialParallelBiasCorrection;
import org.lsst.fits.imageio.cmap.RGBColorMap;
import org.lsst.fits.imageio.cmap.SAOColorMap;

/**
 *
 * @author tonyj
 */
public class CameraImageReader extends ImageReader {

    private static final Logger LOG = Logger.getLogger(CameraImageReader.class.getName());
    private static final CachingReader READER = new CachingReader();
    public static final ImageTypeSpecifier IMAGE_TYPE = ImageTypeSpecifier.createFromBufferedImageType(BufferedImage.TYPE_INT_ARGB);
    public static final RGBColorMap DEFAULT_COLOR_MAP = new SAOColorMap(256, "grey.sao");
    public static final RGBColorMap NULL_COLOR_MAP = new SAOColorMap(256, "null.sao");
    public static final BiasCorrection DEFAULT_BIAS_CORRECTION = new NullBiasCorrection();
    private static final int IMAGE_OFFSET = 100;
    private char wcsString;
    private BiasCorrection biasCorrection;
    private CameraImageReadParam.Scale scale;

    public enum ImageType {
        FOCAL_PLANE, RAFT, CCD
    };

    private boolean showBiasRegion;
    private ImageType imageType;

    static {
        FitsFactory.setUseHierarch(true);
    }

    public CameraImageReader(ImageReaderSpi originatingProvider) {
        super(originatingProvider);
    }

    @Override
    public void reset() {
        super.reset();
    }

    @Override
    public CameraImageReadParam getDefaultReadParam() {
        final CameraImageReadParam fitsImageReadParam = new CameraImageReadParam();
        fitsImageReadParam.setBiasCorrection(new SerialParallelBiasCorrection());
        return fitsImageReadParam;
    }

    @Override
    public void setInput(Object input, boolean seekForwardOnly, boolean ignoreMetadata) {
        super.setInput(input, seekForwardOnly, ignoreMetadata);
        int lines = READER.preReadImage((ImageInputStream) input);
        imageType = lines > 9 ? ImageType.FOCAL_PLANE : lines == 1 ? ImageType.CCD : ImageType.RAFT;
    }

    @Override
    public int getNumImages(boolean allowSearch) throws IOException {
        return 1;
    }

    @Override
    public int getWidth(int imageIndex) throws IOException {
        return switch (imageType) {
            case CCD ->
                509 * 8 + (showBiasRegion ? 1600 : 0);
            case RAFT ->
                3 * 4096 + 4 * IMAGE_OFFSET + (showBiasRegion ? 1600 : 0);
            default ->
                15 * 4096 + 16 * IMAGE_OFFSET;
        };
    }

    @Override
    public int getHeight(int imageIndex) throws IOException {
        return switch (imageType) {
            case CCD ->
                4000 + (showBiasRegion ? 1600 : 0);
            case RAFT ->
                3 * 4096 + 4 * IMAGE_OFFSET + (showBiasRegion ? 1600 : 0);
            default ->
                15 * 4096 + 16 * IMAGE_OFFSET;
        };
    }

    @Override
    public Iterator<ImageTypeSpecifier> getImageTypes(int imageIndex) throws IOException {
        return Collections.singleton(IMAGE_TYPE).iterator();
    }

    @Override
    public IIOMetadata getStreamMetadata() throws IOException {
        return null;
    }

    @Override
    public IIOMetadata getImageMetadata(int imageIndex) throws IOException {
        return null;
    }

    private void initialize(ImageReadParam param) {
        BiasCorrection bc;
        char localWcsString;
        CameraImageReadParam.Scale localScale;

        if (param instanceof CameraImageReadParam cameraParam) {
            bc = cameraParam.getBiasCorrection();
            showBiasRegion = cameraParam.isShowBiasRegions();
            localWcsString = cameraParam.getWCSString();
            localScale = cameraParam.getScale();
        } else {
            bc = DEFAULT_BIAS_CORRECTION;
            showBiasRegion = false;
            localWcsString = ' ';
            localScale = CameraImageReadParam.Scale.AMPLIFIER;
        }
        if (localWcsString == ' ') {
            localWcsString = imageType == ImageType.FOCAL_PLANE ? 'E' : imageType == ImageType.RAFT ? 'Q' : 'B';
        }
        this.wcsString = localWcsString;
        this.biasCorrection = bc;
        this.scale = localScale;
    }

    @Override
    public BufferedImage read(int imageIndex, ImageReadParam param) throws IOException {

        int xSubSampling = 1;
        int ySubSampling = 1;
        if (param != null) {
            LOG.log(Level.INFO, "read called sourceRegion={0}", param.getSourceRegion());
            LOG.log(Level.INFO, "read called destination={0}", param.getDestination());
            LOG.log(Level.INFO, "read called renderSize={0}", param.getSourceRenderSize());
            LOG.log(Level.INFO, "read called xsub={0}", param.getSourceXSubsampling());
            LOG.log(Level.INFO, "read called ysub={0}", param.getSourceYSubsampling());
            LOG.log(Level.INFO, "read called xoffsub={0}", param.getSubsamplingXOffset());
            LOG.log(Level.INFO, "read called yoffsub={0}", param.getSubsamplingYOffset());
            xSubSampling = param.getSourceXSubsampling();
            ySubSampling = param.getSourceYSubsampling();
        }
        initialize(param);

        BufferedImage result;
        Graphics2D g;
        RGBColorMap cmap;
        BiasCorrection bc;
        Map<String, Map<String, Object>> wcsOverride = null;
        Rectangle sourceRegion = param == null ? null : param.getSourceRegion();
        long[] globalScale;
        if (param instanceof CameraImageReadParam cameraParam) {
            //LOG.log(Level.INFO, "CameraImageReadParam colorMap = {0}", cameraParam.getColorMapName());
            cmap = cameraParam.getColorMap();
            bc = cameraParam.getBiasCorrection();
            globalScale = cameraParam.getGlobalScale();
            wcsOverride = cameraParam.getWCSOverride();
        } else {
            //LOG.log(Level.INFO, "CameraImageReadParam use defaults");
            cmap = DEFAULT_COLOR_MAP;
            bc = DEFAULT_BIAS_CORRECTION;
            globalScale = null;
        }

        // Note, graphics and source region being flipped in Y to comply with Camera visualization standards
        if (sourceRegion == null) {
            result = IMAGE_TYPE.createBufferedImage(getWidth(0) / xSubSampling, getHeight(0) / ySubSampling);
            g = result.createGraphics();
            g.translate(0, getHeight(0) / ySubSampling);
            g.scale(1.0 / xSubSampling, -1.0 / ySubSampling);
        } else {
            sourceRegion = new Rectangle(sourceRegion.x, getHeight(0) - sourceRegion.y - sourceRegion.height, sourceRegion.width, sourceRegion.height);
            result = IMAGE_TYPE.createBufferedImage((int) (sourceRegion.getWidth() / xSubSampling), (int) (sourceRegion.getHeight() / ySubSampling));
            g = result.createGraphics();
            g.translate(0, sourceRegion.getHeight() / ySubSampling);
            g.scale(1.0 / xSubSampling, -1.0 / ySubSampling);
            g.translate(-sourceRegion.getX(), -sourceRegion.getY());
        }
        try {
            if (scale == CameraImageReadParam.Scale.AMPLIFIER || globalScale != null) {
                READER.readImage((ImageInputStream) getInput(), sourceRegion, g, cmap, bc, showBiasRegion, wcsString, globalScale, wcsOverride);
            } else {
                READER.readImageWithOnTheFlyGlobalScale((ImageInputStream) getInput(), sourceRegion, g, cmap, bc, showBiasRegion, wcsString, wcsOverride);
            }
            return result;
        } finally {
            g.dispose();
        }
    }

    public Segment getImageMetaDataForPoint(ImageReadParam param, int x, int y) {
        initialize(param);
        Rectangle region = new Rectangle(x, y, 1, 1);
        List<Segment> readSegments = READER.readSegments((ImageInputStream) getInput(), wcsString);
        for (Segment segment : readSegments) {
            if (segment.intersects(region)) {
                return segment;
            }
        }
        return null;
    }

    public Number getPixelForSegment(Segment segment, int x, int y) {
        RawData rawData = READER.getRawData(segment);
        Buffer buffer = rawData.getBuffer();
        int p = segment.getDataSec().x + x + y * segment.getNAxis1();
        return buffer instanceof IntBuffer iBuffer ? iBuffer.get(p) : buffer instanceof FloatBuffer fBuffer ? fBuffer.get(p) : 0;
    }

    public int getRGBForSegment(Segment segment, int x, int y) {
        if (scale == CameraImageReadParam.Scale.GLOBAL) {
            long[] globalScale = READER.getGlobalScale((ImageInputStream) getInput(), biasCorrection, wcsString, null);
            BufferedImage image = READER.getBufferedImage(segment, biasCorrection, globalScale, CameraImageReader.DEFAULT_COLOR_MAP);
            return image.getRGB(x + segment.getDataSec().x, y + segment.getDataSec().y);
        } else {
            BufferedImage image = READER.getBufferedImage(segment, biasCorrection, null, CameraImageReader.DEFAULT_COLOR_MAP);
            return image.getRGB(x + segment.getDataSec().x, y + segment.getDataSec().y);
        }
    }

    public CorrectionFactors getCorrectionFactorForSegment(Segment segment) {
        return READER.getCorrectionFactors(segment, biasCorrection);
    }

    public List<SegmentGeometry> getSegmentGeometry(ImageReadParam param) {
        initialize(param);
        List<Segment> readSegments = READER.readSegments((ImageInputStream) getInput(), wcsString);
        return readSegments.stream().map(s -> new SegmentGeometry(s)).collect(Collectors.toList());
    }

    public static class SegmentGeometry {

        private final int x;
        private final int y;
        private final int width;
        private final int height;
        private final String segmentName;
        private final String raftBay;
        private final String ccdSlot;
        private final double[] flatmatrix;

        public SegmentGeometry(Segment segment) {
            Rectangle2D.Double wcs = segment.getWcs();
            this.x = (int) Math.round(wcs.x);
            this.y = (int) Math.round(wcs.y);
            this.width = (int) Math.round(wcs.width);
            this.height = (int) Math.round(wcs.height);
            this.segmentName = segment.getSegmentName();
            this.raftBay = segment.getRaftBay();
            this.ccdSlot = segment.getCcdSlot();
            this.flatmatrix = new double[6];
            AffineTransform wcsTranslation = segment.getWCSTranslation(false);
            try {
                AffineTransform inverse = wcsTranslation.createInverse();
                inverse.getMatrix(flatmatrix);
            } catch (NoninvertibleTransformException ex) {
                // Should never happen
            }
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }

        public int getWidth() {
            return width;
        }

        public int getHeight() {
            return height;
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

        public double[] getFlatmatrix() {
            return flatmatrix;
        }

    }
}
