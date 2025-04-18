package org.lsst.fits.imageio.cmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author tonyj
 */
public class SAOColorMapTest {

    @Test
    public void testB() {
        SAOColorMap b = new SAOColorMap(256, "b.sao");
        assertEquals(0, b.getRGB(0));
        assertEquals(0xff0200, b.getRGB(128));
        assertEquals(0xffffff, b.getRGB(255));
    }

    @Test
    public void testCubeHelix() {
        SAOColorMap cubeHelix = new SAOColorMap(256, "cubehelix00.sao");
        assertEquals(0, cubeHelix.getRGB(0));
        assertEquals(0xa1794a, cubeHelix.getRGB(128));
        assertEquals(0xffffff, cubeHelix.getRGB(255));
    }

    @Test
    public void testMissing() {
        try {
            SAOColorMap saoColorMap = new SAOColorMap(256, "missing.sao");
            fail("should not reach here: " + saoColorMap);
        } catch (RuntimeException x) {
            assertTrue(x.getMessage().contains("missing.sao"));
        }
    }
}
