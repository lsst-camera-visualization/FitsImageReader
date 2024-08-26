package org.lsst.fits.imageio;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.imageio.ImageReadParam;
import org.lsst.fits.imageio.bias.BiasCorrection;
import org.lsst.fits.imageio.bias.SerialParallelBiasCorrection;
import org.lsst.fits.imageio.bias.SerialParallelBiasSub;
import org.lsst.fits.imageio.bias.SerialParallelBiasSubtraction;
import org.lsst.fits.imageio.bias.SerialParallelBiasSubtraction2;
import org.lsst.fits.imageio.cmap.RGBColorMap;
import org.lsst.fits.imageio.cmap.SAOColorMap;

/**
 *
 * @author tonyj
 */
public class CameraImageReadParam extends ImageReadParam {

    private boolean showBiasRegions = false;
    private final GetSetAvailable<BiasCorrection> bc;
    private final GetSetAvailable<RGBColorMap> colorMap;
    private char wcsString = ' ';
    private long[] globalScale;
    private Map<String, Map<String, Object>> wcsOverride = null;

    public enum Scale {
        GLOBAL, AMPLIFIER
    };
    private Scale scale = Scale.AMPLIFIER;

    public CameraImageReadParam() {
        Map<String, BiasCorrection> biasCorrectionOptions = new LinkedHashMap<>();
        biasCorrectionOptions.put("None", CameraImageReader.DEFAULT_BIAS_CORRECTION);
        biasCorrectionOptions.put("Simple Overscan Correction", new SerialParallelBiasCorrection());
        biasCorrectionOptions.put("Simple Overscan Subtraction", new SerialParallelBiasSubtraction());
        biasCorrectionOptions.put("Simple Overscan Subtraction2", new SerialParallelBiasSubtraction2());
        biasCorrectionOptions.put("Simple Overscan Subtraction only", new SerialParallelBiasSub());
        bc= new GetSetAvailable<>(CameraImageReader.DEFAULT_BIAS_CORRECTION, "Bias Correction", biasCorrectionOptions);
 
        Map<String, RGBColorMap>  colorMapOptions = new LinkedHashMap<>();
        colorMapOptions.put("grey", new SAOColorMap(256, "grey.sao"));
        colorMapOptions.put("a", new SAOColorMap(256, "a.sao"));
        colorMapOptions.put("b", new SAOColorMap(256, "b.sao"));
        colorMapOptions.put("bb", new SAOColorMap(256, "bb.sao"));
        colorMapOptions.put("cubehelix0", new SAOColorMap(256, "cubehelix0.sao"));
        colorMapOptions.put("cubehelix1", new SAOColorMap(256, "cubehelix1.sao"));
        colorMapOptions.put("rainbow", new SAOColorMap(256, "rainbow.sao"));
        colorMapOptions.put("standard", new SAOColorMap(256, "standard.sao"));
        colorMapOptions.put("null", new SAOColorMap(256, "null.sao"));
        colorMap = new GetSetAvailable<>(CameraImageReader.DEFAULT_COLOR_MAP, "Color Map", colorMapOptions);
    }

    public boolean isShowBiasRegions() {
        return showBiasRegions;
    }

    public void setShowBiasRegions(boolean showBiasRegions) {
        this.showBiasRegions = showBiasRegions;
    }

    public char getWCSString() {
        return wcsString;
    }

    public void setWCSString(char wcsString) {
        this.wcsString = wcsString;
    }

    public RGBColorMap getColorMap() {
        return colorMap.getValue();
    }

    public void setColorMap(RGBColorMap colorMap) {
        this.colorMap.setValue(colorMap);
    }

    public Set<String> getAvailableColorMaps() {
        return colorMap.getAvailable();
    }

    public void setColorMap(String name) {
        colorMap.setValue(name);

    }

    public String getColorMapName() {
        return colorMap.getValueName();
    }

    public BiasCorrection getBiasCorrection() {
        return bc.getValue();
    }

    public void setBiasCorrection(BiasCorrection bc) {
        this.bc.setValue(bc);
    }

    public Set<String> getAvailableBiasCorrections() {
        return bc.getAvailable();
    }

    public void setBiasCorrection(String name) {
        bc.setValue(name);
    }

    public String getBiasCorrectionName() {
        return bc.getValueName();
    }

    public long[] getGlobalScale() {
        return globalScale;
    }

    public void setGlobalScale(long[] globalScale) {
        this.globalScale = globalScale;
    }

    public Map<String, Map<String, Object>> getWCSOverride() {
        return wcsOverride;
    }

    public void setWCSOverride(Map<String, Map<String, Object>> wcsOverride) {
        this.wcsOverride = wcsOverride;
    }

    public Scale getScale() {
        return scale;
    }

    public void setScale(Scale scale) {
        this.scale = scale;
    }

    private static class GetSetAvailable<T> {

        private T value;
        private final Map<String, T> available;
        private final String type;

        public GetSetAvailable(T initial, String type, Map<String, T> available) {
            this.value = initial;
            this.available = available;
            this.type = type;
        }

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
        }

        public Set<String> getAvailable() {
            return Collections.unmodifiableSet(available.keySet());
        }

        public void setValue(String name) {
            if (!available.containsKey(name)) {
                throw new IllegalArgumentException("Unknown " + type + ": " + name);
            }
            setValue(available.get(name));
        }

        public String getValueName() {
            for (Map.Entry<String, T> entry : available.entrySet()) {
                if (Objects.equals(entry.getValue(), value)) {
                    return entry.getKey();
                }
            }
            return null;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 97 * hash + Objects.hashCode(this.value);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final GetSetAvailable<?> other = (GetSetAvailable<?>) obj;
            return Objects.equals(this.value, other.value);
        }
    }
}
