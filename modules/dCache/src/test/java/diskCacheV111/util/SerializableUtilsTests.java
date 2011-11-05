package diskCacheV111.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

public class SerializableUtilsTests {

    private static final String EXAMPLE_1_PNFSID =
        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
      + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
      + "7200025b42acf317f8060854e002000078700000000c000f0000000000000"
      + "0389fc0707400183030304630303030303030303030303030303338394643"
      + "3071007e0006";

    private static final String EXAMPLE_2_PNFSID =
        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
      + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
      + "7200025b42acf317f8060854e002000078700000000c000f0000000000000"
      + "0389fc0740006646f6d61696e740018303030463030303030303030303030"
      + "30303033383946433074001f3030304630303030303030303030303030303"
      + "338394643302e646f6d61696e";

    private static final String EXAMPLE_3_PNFSID =
        "aced0005737200196469736b4361636865563131312e7574696c2e506e667"
      + "34964fe7150258b9baecf0200045b00025f617400025b424c00075f646f6d"
      + "61696e7400124c6a6176612f6c616e672f537472696e673b4c00095f69645"
      + "37472696e6771007e00024c00095f746f537472696e6771007e0002787075"
      + "7200025b42acf317f8060854e002000078700000001280d1b8b90ced30430"
      + "608c58002811b3285fc707400243830443142384239304345443330343330"
      + "3630384335383030323831314233323835464371007e0006";

    @Test
    public void testDecodeEncodeExample1() throws IOException {
        assertDecodeEncodeEquals( EXAMPLE_1_PNFSID);
    }

    @Test
    public void testDecodeEncodeExample2() throws IOException {
        assertDecodeEncodeEquals( EXAMPLE_2_PNFSID);
    }

    @Test
    public void testDecodeEncodeExample3() throws IOException {
        assertDecodeEncodeEquals( EXAMPLE_3_PNFSID);
    }

    @Test
    public void testEncodeDecodeExample1() throws IOException {
        byte[] rawSimple = SerializableUtils.decode( EXAMPLE_1_PNFSID);
        assertEncodeDecodeEquals( rawSimple);
    }

    @Test
    public void testEncodeDecodeExample2() throws IOException {
        byte[] rawDomain = SerializableUtils.decode( EXAMPLE_2_PNFSID);
        assertEncodeDecodeEquals( rawDomain);
    }

    @Test
    public void testEncodeDecodeExample3() throws IOException {
        byte[] rawChimera = SerializableUtils.decode( EXAMPLE_3_PNFSID);
        assertEncodeDecodeEquals( rawChimera);
    }


    private void assertEncodeDecodeEquals( byte[] data) {
        String encodedData = SerializableUtils.encode( data);
        byte[] decodedEncodedData = SerializableUtils.decode( encodedData);
        assertArrayEquals(
                "mismatch between data and decoded version of encoded data",
                data, decodedEncodedData);
    }

    private void assertDecodeEncodeEquals( String data) {
        byte[] decodedData = SerializableUtils.decode( data);
        String encodedDecodedData = SerializableUtils.encode( decodedData);
        assertEquals(
                "mismatch between data and encoded version of decoded data",
                data, encodedDecodedData);
    }
}
