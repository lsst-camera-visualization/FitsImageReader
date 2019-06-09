package org.lsst.fits.imageio.bias;

import java.nio.IntBuffer;
import org.lsst.fits.imageio.Segment;

/**
 *
 * @author tonyj
 */
public class NullBiasCorrection implements BiasCorrection {

    private static final CorrectionFactors NOOP_CORRECTION = (int x, int y) -> 0;
    @Override
    public CorrectionFactors compute(IntBuffer data, Segment segment) {
        return NOOP_CORRECTION;
    }
    
}
