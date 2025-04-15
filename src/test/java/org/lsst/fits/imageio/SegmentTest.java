package org.lsst.fits.imageio;

import java.io.File;
import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;
import nom.tam.fits.TruncatedFileException;
import nom.tam.util.BufferedFile;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lsst.fits.imageio.bias.BiasCorrection;
import org.lsst.fits.imageio.bias.SerialParallelBiasSubtraction2.SimpleCorrectionFactors;
import org.lsst.fits.imageio.bias.SerialParallelBiasSubtraction2;

/**
 *
 * @author tonyj
 */
public class SegmentTest {
    
    private static File testFile = new File("/home/tonyj/Data/pretty/20_Flat_screen_0000_20190322172301.fits");
    private static File testFileCompressed = new File("/home/tonyj/Data/pretty/20_Flat_screen_0000_20190322172301.fits.fz");

    
    public SegmentTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        Assume.assumeTrue(testFile.exists());
        Assume.assumeTrue(testFileCompressed.exists());
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testSegment() throws IOException, TruncatedFileException, FitsException {
        File file = testFile;
        BufferedFile bf = new BufferedFile(file, "r");
        @SuppressWarnings("UnusedAssignment")
        Header header = new Header(bf); // Skip primary header
        for (int i = 0; i < 11; i++) {
            header = new Header(bf);
            bf.seek(bf.getFilePointer() + header.getDataSize());
        }
        header = new Header(bf);
        Segment segment = new Segment(header, 12, file.length(), file, bf, "R22", "S20", '4', null);
        IntBuffer intBuffer = (IntBuffer) segment.readRawDataAsync(null).join().getBuffer();

        BiasCorrection bc = new SerialParallelBiasSubtraction2();
        BiasCorrection.CorrectionFactors factors = bc.compute(intBuffer, segment);
        assertTrue(factors instanceof SimpleCorrectionFactors);
        SimpleCorrectionFactors simple = (SimpleCorrectionFactors) factors;
        assertEquals(-2583, simple.getOverallCorrection());   
    }

    @Test
    public void testCompressedSegment() throws IOException, TruncatedFileException, FitsException {
        File file = testFileCompressed;
        BufferedFile bf = new BufferedFile(file, "r");
        @SuppressWarnings("UnusedAssignment")
        Header header = new Header(bf); // Skip primary header
        for (int i = 0; i < 11; i++) {
            header = new Header(bf);
            bf.seek(bf.getFilePointer() + header.getDataSize());
        }
        header = new Header(bf);
        Segment segment = new Segment(header, 12, file.lastModified(), file, bf, "R22", "S20", '4', null);
        IntBuffer intBuffer = (IntBuffer) segment.readRawDataAsync(null).join().getBuffer();

        BiasCorrection bc = new SerialParallelBiasSubtraction2();
        BiasCorrection.CorrectionFactors factors = bc.compute(intBuffer, segment);
        assertTrue(factors instanceof SimpleCorrectionFactors);
        SimpleCorrectionFactors simple = (SimpleCorrectionFactors) factors;
        assertEquals(-2583, simple.getOverallCorrection());   
    }

    @Test
    public void testSwitchSegment() throws IOException, TruncatedFileException, FitsException {
        File file = File.createTempFile("test", "fits");
        Path path = file.toPath();
        Files.delete(path);
        Files.createSymbolicLink(path, testFile.toPath());
        BufferedFile bf = new BufferedFile(file, "r");
        @SuppressWarnings("UnusedAssignment")
        Header header = new Header(bf); // Skip primary header
        for (int i = 0; i < 11; i++) {
            header = new Header(bf);
            bf.seek(bf.getFilePointer() + header.getDataSize());
        }
        header = new Header(bf);
        Segment segment = new Segment(header, 12, file.lastModified(), file, bf, "R22", "S20", '4', null);
        // Switcheroo
        Files.delete(path);
        Files.createSymbolicLink(path, testFileCompressed.toPath());        
        IntBuffer intBuffer = (IntBuffer) segment.readRawDataAsync(null).join().getBuffer();

        BiasCorrection bc = new SerialParallelBiasSubtraction2();
        BiasCorrection.CorrectionFactors factors = bc.compute(intBuffer, segment);
        assertTrue(factors instanceof SimpleCorrectionFactors);
        SimpleCorrectionFactors simple = (SimpleCorrectionFactors) factors;
        assertEquals(-2583, simple.getOverallCorrection());   
    }    
}
