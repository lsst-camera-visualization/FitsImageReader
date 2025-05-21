package org.lsst.fits.imageio;

import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.Random;
import org.lsst.fits.imageio.cmap.RGBColorMap;

/**
 * The goal of this class is to scale a given set of arbitrary floating or
 * integer data to a range of RGB values. We do this by histograming the data
 * and then computing a cdf so that the output RGB value is scaled according to
 * the frequency of occurrence of the value in the input data.
 *
 * @author tonyj
 */
public class EnhancedScalingUtils {

    private final static int MAX_BINS = 100000;

    private float min;
    private float max;
    private float binSize;
    private final int[] histogram;
    private int nEntries;
    private final int[] rgb;

    EnhancedScalingUtils(FloatBuffer data, RGBColorMap colorMap) {
        histogram = fillHistogram(MAX_BINS, data);
        rgb = computeCDF(histogram, nEntries, colorMap);
    }

    private int[] fillHistogram(int bins, FloatBuffer data) {
        // Make initial guess of range
        float min = 0;
        float max = 1;
        // Create the histogram
        int[] histogram = new int[bins];
        float binSize = (max - min) / bins;
        int nEntries = 0;

        while (data.hasRemaining()) {
            float f = data.get();
            int bin = binFor(min, binSize, f);
            if (bin >= bins) {
                // Always increase the binSize by an integer factor
                int rebinFactor = (int) Math.ceil((min - f) / (min - max));
                int k = 0;
                for (int i = 0; i < bins; i += rebinFactor) {
                    int newCount = 0;
                    for (int j = 0; j < rebinFactor; j++) {
                        if (i + j < bins) {
                            newCount += histogram[i + j];
                        }
                    }
                    histogram[k++] = newCount;
                }
                Arrays.fill(histogram, k, bins, 0);
                binSize *= rebinFactor;
                max = min + bins * binSize;
                bin = binFor(min, binSize, f);

            } else if (bin < 0) {
                // Always increase the binSize by an integer factor
                int rebinFactor = (int) Math.ceil((f - max) / (min - max));
                int k = bins;
                for (int i = bins - rebinFactor; i > 0 - rebinFactor; i -= rebinFactor) {
                    int newCount = 0;
                    for (int j = 0; j < rebinFactor; j++) {
                        if (i + j >= 0) {
                            newCount += histogram[i + j];
                        }
                    }
                    histogram[--k] = newCount;
                }
                Arrays.fill(histogram, 0, k, 0);
                binSize *= rebinFactor;
                min = max - bins * binSize;
                bin = binFor(min, binSize, f);
            }
            histogram[bin]++;
            nEntries++;
        }   
        this.min = min;
        this.max = max;
        this.binSize = binSize;
        this.nEntries = nEntries;
        return histogram;
    }
    
    private int[] computeCDF(int[] histogram, int nEntries, RGBColorMap colorMap) {
        int[] cdf = new int[histogram.length];
        int size = colorMap.getSize() - 1;
        float cum = 0;
        for (int i = 0; i < histogram.length; i++) {
            cum += histogram[i];
            cdf[i] = colorMap.getRGB((int) Math.floor(size * cum / nEntries));
        }
        return cdf;
    }

    private int binFor(float min, float binSize, float f) {
        return (int) Math.floor((f - min) / binSize);
    }

    public float getMin() {
        return min;
    }

    public float getMax() {
        return max;
    }

    public float getBinSize() {
        return binSize;
    }

    public int[] getHistogram() {
        return histogram;
    }

    public int getEntries() {
        return nEntries;
    }

    int getPseudoRGB(float value) {
        // Return a pseudo 18-bit rgb following Jeff's conventions
        int bin = (int) Math.floor((value - min) / binSize / histogram.length * (2 ^ 18));
        return bin << 6;
    }
    
    int getRGB(float value) {
        return rgb[binFor(min, binSize, value)];
    }

    @Override
    public String toString() {
        return "EnhancedScalingUtils{" + "min=" + min + ", max=" + max + ", binSize=" + binSize + ", nEntries=" + nEntries + 
                "\n histogram=" + Arrays.toString(histogram) +"\n rgb=" + Arrays.toString(rgb) + '}';
    }

    public static void main(String[] args) {
        FloatBuffer buffer = FloatBuffer.allocate(1000);
        Random random = new Random();
        while (buffer.hasRemaining()) {
            buffer.put((float) random.nextGaussian(10, 3));
        }
        buffer.flip();
        EnhancedScalingUtils esu = new EnhancedScalingUtils(buffer, CameraImageReader.DEFAULT_COLOR_MAP);
        System.out.println(esu);
        int count = Arrays.stream(esu.getHistogram()).sum();
        System.out.println(count);
    }

}
