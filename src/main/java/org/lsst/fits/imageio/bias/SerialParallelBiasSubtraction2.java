package org.lsst.fits.imageio.bias;

import java.awt.Rectangle;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Arrays;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;
import nom.tam.fits.TruncatedFileException;
import nom.tam.util.FitsFile;
import org.lsst.fits.imageio.Segment;

/**
 *
 * @author tonyj
 */
public class SerialParallelBiasSubtraction2 implements BiasCorrection {

    private final int targetBiasLevel = 20000;

    @Override
    public CorrectionFactors compute(IntBuffer data, Segment segment) {

        int nAxis1 = segment.getNAxis1();
        int nAxis2 = segment.getNAxis2();
        Rectangle datasec = segment.getDataSec();

        // Deal with serial overscan
        int[] serialBias = new int[datasec.height];
        int averageSerialBias = 0;
        int serialOverscanStart = datasec.x + datasec.width + 4;
        int position = 0;
        for (int y = datasec.y; y < datasec.height + datasec.y; y++) {
            int biasSum = 0;
            for (int x = serialOverscanStart; x < nAxis1; x++) {
                biasSum += data.get(position + x);
            }
            biasSum /= nAxis1 - serialOverscanStart;
            serialBias[y - datasec.y] = biasSum;
            averageSerialBias += biasSum;
            position += nAxis1;
        }
        averageSerialBias /= datasec.height;
        for (int i = 0; i < serialBias.length; i++) {
            serialBias[i] -= averageSerialBias;
        }

        // Deal with parallel overscan
        int[] parallelBias = new int[datasec.width];
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
            if (biasSum > 100000 || biasSum < 10000) {
                biasSum = prevBiasSum;
            }
            parallelBias[x - datasec.x] = biasSum;
            averageParallelBias += biasSum;
            position += nAxis1;
            prevBiasSum = biasSum;
        }
        averageParallelBias /= datasec.width;
        for (int i = 0; i < parallelBias.length; i++) {
            parallelBias[i] -= averageParallelBias;
        }

        int overallCorrection = targetBiasLevel - (averageSerialBias*datasec.height + averageParallelBias*datasec.width) / (datasec.height + datasec.width);
        final SimpleCorrectionFactors simpleCorrectionFactors = new SimpleCorrectionFactors(datasec, overallCorrection, serialBias, parallelBias);
        return simpleCorrectionFactors;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && this.getClass().equals(obj.getClass());
    }

    @Override
    public int hashCode() {
        return SerialParallelBiasSubtraction2.class.hashCode();
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

        BiasCorrection bc = new SerialParallelBiasSubtraction2();
        CorrectionFactors factors = bc.compute(intBuffer, segment);
        System.out.println(factors);
    }

    public static class SimpleCorrectionFactors implements CorrectionFactors {

        private final Rectangle datasec;
        private final int[] serialBias;
        private final int[] parallelBias;
        private final int overallCorrection;

        private SimpleCorrectionFactors(Rectangle datasec, int overallCorrection, int[] serialBias, int[] parallelBias) {
            this.datasec = datasec;
            this.serialBias = serialBias;
            this.parallelBias = parallelBias;
            this.overallCorrection = overallCorrection;
        }

        @Override
        public int correctionFactor(int x, int y) {
            return -overallCorrection + serialBias[y - datasec.y] + parallelBias[x - datasec.x];
        }

        @Override
        public String toString() {
            double sAvg = Arrays.stream(serialBias).average().getAsDouble();
            double pAvg = Arrays.stream(parallelBias).average().getAsDouble();

            return "SimpleCorrectionFactors{" + "savg=" + sAvg + ", pavg="+ pAvg + ", datasec=" + datasec + ", serialBias=" + Arrays.toString(serialBias) + ", parallelBias=" + Arrays.toString(parallelBias) + ", overallCorrection=" + overallCorrection + '}';
        }

    }
}
