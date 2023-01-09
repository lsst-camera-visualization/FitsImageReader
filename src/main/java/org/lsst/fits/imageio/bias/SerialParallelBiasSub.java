package org.lsst.fits.imageio.bias;

import java.awt.Rectangle;
import java.io.IOException;
import java.nio.IntBuffer;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;
import nom.tam.fits.TruncatedFileException;
import nom.tam.util.FitsFile;
import org.lsst.fits.imageio.Segment;

/**
 *
 * @author tonyj
 */
public class SerialParallelBiasSub implements BiasCorrection {

    private final int targetBiasLevel = 20000;

    @Override
    public CorrectionFactors compute(IntBuffer data, Segment segment) {

        int nAxis1 = segment.getNAxis1();
        int nAxis2 = segment.getNAxis2();
        Rectangle datasec = segment.getDataSec();

        int averageSerialBias = 0;
        int serialOverscanStart = datasec.x + datasec.width + 4;
        int position = 0;
        for (int y = datasec.y; y < datasec.height + datasec.y; y++) {
            int biasSum = 0;
            for (int x = serialOverscanStart; x < nAxis1; x++) {
                biasSum += data.get(position + x);
            }
            biasSum /= nAxis1 - serialOverscanStart;
            averageSerialBias += biasSum;
            position += nAxis1;
        }
        //averageSerialBias /= datasec.height;

        // Deal with parallel overscan
        int averageParallelBias = 0;

        int parallelOverscanStart = datasec.y + datasec.height + 4;

        int prevBiasSum = 22000;
        for (int x = datasec.x; x < datasec.width + datasec.x; x++) {
            int biasSum = 0;
            for (int y = parallelOverscanStart; y < nAxis2; y++) {
                biasSum += data.get(x + y * nAxis1);
            }
            biasSum /= nAxis2 - parallelOverscanStart;
            // protect against outliers
            if (biasSum > 100000) {
                biasSum = prevBiasSum;
            }
            averageParallelBias += biasSum;
            position += nAxis1;
            prevBiasSum = biasSum;
        }
        //averageParallelBias /= datasec.width;

        int overallCorrection = targetBiasLevel - (averageSerialBias + averageParallelBias) / (datasec.width + datasec.height);
        final SimpleCorrectionFactors simpleCorrectionFactors = new SimpleCorrectionFactors(datasec, overallCorrection);
        return simpleCorrectionFactors;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && this.getClass().equals(obj.getClass());
    }

    @Override
    public int hashCode() {
        return SerialParallelBiasSub.class.hashCode();
    }

    public static void main(String[] args) throws IOException, TruncatedFileException, FitsException {
        String file = "/home/tonyj/Data/pretty/20_Flat_screen_0000_20190322172301.fits";
        FitsFile bf = new FitsFile(file, "r");
        @SuppressWarnings("UnusedAssignment")
        Header header = new Header(bf); // Skip primary header
        for (int i = 0; i < 11; i++) {
            header = new Header(bf);
            bf.seek(bf.getFilePointer() + header.getDataSize());
        }
        header = new Header(bf);
        Segment segment = new Segment(header, file, bf, "R22", "S20", '4', null);
        IntBuffer intBuffer = (IntBuffer) segment.readRawDataAsync(null).join().getBuffer();

        BiasCorrection bc = new SerialParallelBiasSub();
        CorrectionFactors factors = bc.compute(intBuffer, segment);
        System.out.println(factors);
    }

    public static class SimpleCorrectionFactors implements CorrectionFactors {

        private final Rectangle datasec;
        private final int overallCorrection;

        private SimpleCorrectionFactors(Rectangle datasec, int overallCorrection) {
            this.datasec = datasec;
            this.overallCorrection = overallCorrection;
        }

        @Override
        public int correctionFactor(int x, int y) {
            return -overallCorrection;
        }

        @Override
        public String toString() {
            return "SimpleCorrectionFactors{" + "datasec=" + datasec + ", overallCorrection=" + overallCorrection + '}';
        }

    }

    void streamSerialOverscan(IntBuffer data, Segment segment, int rowsToSkip, Callback callback) {
        Rectangle datasec = segment.getDataSec();
        int nAxis1 = segment.getNAxis1();
        int serialOverscanStart = datasec.x + datasec.width + rowsToSkip;
        int position = 0;
        for (int y = datasec.y; y < datasec.height + datasec.y; y++) {
            for (int x = serialOverscanStart; x < nAxis1; x++) {
                callback.apply(x, y, data.get(position + x));
            }
            position += nAxis1;
        }
        
    }

    
    void streamParallelOverscan(IntBuffer data, Segment segment, int colsToSkip, Callback callback) {
        Rectangle datasec = segment.getDataSec();
        int nAxis1 = segment.getNAxis1();
        int nAxis2 = segment.getNAxis2();
        int parallelOverscanStart = datasec.y + datasec.height + colsToSkip;

        for (int x = datasec.x; x < datasec.width + datasec.x; x++) {
            for (int y = parallelOverscanStart; y < nAxis2; y++) {
               callback.apply(x, y, data.get(x + y * nAxis1));
            }
        }        
    }
    
    private static interface Callback {
         void apply(int x, int y, int pixel);
    }
}
