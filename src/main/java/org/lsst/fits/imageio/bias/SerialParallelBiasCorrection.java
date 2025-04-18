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
public class SerialParallelBiasCorrection implements BiasCorrection {

    @Override
    public CorrectionFactors compute(IntBuffer data, Segment segment) {

        int nAxis1 = segment.getNAxis1();
        int nAxis2 = segment.getNAxis2();
        Rectangle datasec = segment.getDataSec();

        // Deal with serial overscan
        int[] serialBias = new int[datasec.height];
        int minSerialBias = 999999;
        int serialOverscanStart = datasec.x + datasec.width + 4;
        int position = 0;
        for (int y = datasec.y; y < datasec.height + datasec.y; y++) {
            int biasSum = 0;
            for (int x = serialOverscanStart; x < nAxis1; x++) {
                biasSum += data.get(position + x);
            }
            biasSum /= nAxis1 - serialOverscanStart;
            serialBias[y - datasec.y] = biasSum;
            minSerialBias = Math.min(minSerialBias, biasSum);
            position += nAxis1;
        }
        for (int i = 0; i < serialBias.length; i++) {
            serialBias[i] -= minSerialBias;
        }

        // Deal with parallel overscan
        int[] parallelBias = new int[datasec.width];
        int minParallelBias = 999999;

        int parallelOverscanStart = datasec.y + datasec.height + 4;

        for (int x = datasec.x; x < datasec.width + datasec.x; x++) {
            int biasSum = 0;
            for (int y = parallelOverscanStart; y < nAxis2; y++) {
                biasSum += data.get(x + y * nAxis1);
            }
            biasSum /= nAxis2 - parallelOverscanStart;
            parallelBias[x - datasec.x] = biasSum;
            minParallelBias = Math.min(minParallelBias, biasSum);
            position += nAxis1;
        }
        for (int i = 0; i < parallelBias.length; i++) {
            parallelBias[i] -= minParallelBias;
        }

        return new SimpleCorrectionFactors(datasec, serialBias, parallelBias);
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && this.getClass().equals(obj.getClass());
    }

    @Override
    public int hashCode() {
        return SerialParallelBiasCorrection.class.hashCode();
    }

    public static void main(String[] args) throws IOException, TruncatedFileException, FitsException {
        String file = "/home/tonyj/Data/pretty/11_Flat_screen_0000_20190322172301.fits";
        FitsFile bf = new FitsFile(file, "r");
        @SuppressWarnings("UnusedAssignment")
        Header header = new Header(bf); // Skip primary header
        header = new Header(bf);

        Segment segment = new Segment(header, 1, file.length(), file, bf, "R22", "S11", 'Q', null);
        IntBuffer intBuffer = (IntBuffer) segment.readRawDataAsync(null).join().getBuffer();

        BiasCorrection bc = new SerialParallelBiasCorrection();
        CorrectionFactors factors = bc.compute(intBuffer, segment);
        System.out.println(factors);
    }

    public static class SimpleCorrectionFactors implements CorrectionFactors {

        private final Rectangle datasec;
        private final int[] serialBias;
        private final int[] parallelBias;

        private SimpleCorrectionFactors(Rectangle datasec, int[] serialBias, int[] parallelBias) {
            this.datasec = datasec;
            this.serialBias = serialBias;
            this.parallelBias = parallelBias;
        }

        @Override
        public int correctionFactor(int x, int y) {
            return serialBias[y - datasec.y] + parallelBias[x - datasec.x];
        }

        @Override
        public String toString() {
            return "CorrectionFactors{" + "datasec=" + datasec + ", serialBias=" + Arrays.toString(serialBias) + ", parallelBias=" + Arrays.toString(parallelBias) + '}';
        }
    }
}
