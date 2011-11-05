package org.dcache.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dcache.util.SimpleGSIEngine.GSIEngineResult;
import org.dcache.util.SimpleGSIEngine.HandshakeStatus;
import org.dcache.util.SimpleGSIEngine.ResultStatus;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.junit.Before;
import org.junit.Test;

public class SimpleGSIEngineTest {
    private final static int SSLV3_PAYLOAD_MAX_SIZE = 32*1024;
    private final static int GSI_PAYLOAD_MAX_SIZE = 32*1024*1024;
    private final static int TCP_BUFFER_SIZE = 1448;
    private final static int MAX_HANDSHAKE_ITERATIONS = 10;

    private SimpleGSIEngine _testEngine;

    @Before
    public void setUp() {
       GSSContext context = SimpleGSIEngineHelper.newGSSContext();
       _testEngine = new SimpleGSIEngine(context);
    }

    @Test
    public void testSSLv3DestBufferOverflow() throws GSSException, IOException {
        ByteBuffer smallDestBuffer = ByteBuffer.allocate(7);
        ByteBuffer bigSrcBuffer = ByteBuffer.allocate(255);

        byte [] inputString = "Hello world".getBytes();
        byte [] header = SimpleGSIEngineHelper.getSSLV3Header((short)inputString.length);

        bigSrcBuffer.put(header).put(inputString).flip();

        GSIEngineResult result = _testEngine.handshake(bigSrcBuffer,
                                                       smallDestBuffer);
        assertEquals(ResultStatus.BUFFER_OVERFLOW, result.getStatus());
    }

    @Test
    public void testSSLv3SrcBufferUnderflow() throws GSSException, IOException {
        ByteBuffer incompleteSrcBuffer = ByteBuffer.allocate(6);
        ByteBuffer dummyDestBuffer = ByteBuffer.allocate(1);

        byte [] header = SimpleGSIEngineHelper.getSSLV3Header((short)20);

        incompleteSrcBuffer.put(header).put((byte) 0).flip();

        GSIEngineResult result = _testEngine.handshake(incompleteSrcBuffer,
                                                       dummyDestBuffer);
        assertEquals(ResultStatus.BUFFER_UNDERFLOW, result.getStatus());
    }

    @Test
    public void testSSLv3Handshake() throws GSSException, IOException {
        ByteBuffer srcBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);
        ByteBuffer dstBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);

        byte [] inputString = "Hello world".getBytes();
        byte [] header = SimpleGSIEngineHelper.getSSLV3Header(inputString.length);

        srcBuffer.put(header).put(inputString).flip();

        GSIEngineResult result = _testEngine.handshake(srcBuffer, dstBuffer);

        assertEquals(result.getStatus(), ResultStatus.OK);
        assertFalse("Handshake reported success, but no progress was made!",
                    _testEngine.getHandshakeStatus().equals(HandshakeStatus.CH_INITIAL));
    }

    @Test
    public void testSSLv3WrapUnwrap() throws GSSException, IOException {
        ByteBuffer srcBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);
        ByteBuffer dstBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);

        byte [] inputString = "Hello world".getBytes();
        byte [] header = SimpleGSIEngineHelper.getSSLV3Header(inputString.length);

        prepareEngineForUnwrap(header, inputString);

        /* no header, should be added by wrap */
        srcBuffer.put(inputString).flip();
        /* now let's go and wrap the information */
        GSIEngineResult result = _testEngine.wrap(srcBuffer, dstBuffer);
        dstBuffer.flip();
        ByteBuffer dstBuffer2 = ByteBuffer.allocate(TCP_BUFFER_SIZE);

        result = _testEngine.unwrap(dstBuffer, dstBuffer2);
        dstBuffer2.flip();
        byte [] unwrapResult = new byte[result.getBytesProduced()];
        dstBuffer2.get(unwrapResult);

        assertArrayEquals(inputString, unwrapResult);
    }

    @Test
    public void testSSLv2DestBufferOverflow() throws GSSException, IOException {
        ByteBuffer smallDestBuffer = ByteBuffer.allocate(7);
        ByteBuffer bigSrcBuffer = ByteBuffer.allocate(255);

        byte [] inputString = "Hello world".getBytes();
        byte [] header = SimpleGSIEngineHelper.getSSLV2Header(inputString.length);

        bigSrcBuffer.put(header).put(inputString).flip();

        GSIEngineResult result = _testEngine.handshake(bigSrcBuffer,
                                                       smallDestBuffer);
        assertEquals(ResultStatus.BUFFER_OVERFLOW, result.getStatus());
    }

    @Test
    public void testSSLv2SrcBufferUnderflow() throws GSSException, IOException {
        ByteBuffer incompleteSrcBuffer = ByteBuffer.allocate(5);
        ByteBuffer dummyDestBuffer = ByteBuffer.allocate(1);

        byte [] header = SimpleGSIEngineHelper.getSSLV2Header(20);

        incompleteSrcBuffer.put(header).put((byte) 0).flip();

        GSIEngineResult result = _testEngine.handshake(incompleteSrcBuffer,
                                                       dummyDestBuffer);
        assertEquals(ResultStatus.BUFFER_UNDERFLOW, result.getStatus());
    }

    @Test
    public void testGSIDestBufferOverflow() throws GSSException, IOException {
        ByteBuffer smallDestBuffer = ByteBuffer.allocate(7);
        ByteBuffer bigSrcBuffer = ByteBuffer.allocate(255);

        byte [] inputString = "Hello world".getBytes();
        byte [] header = SimpleGSIEngineHelper.getGSIHeader(inputString.length);

        bigSrcBuffer.put(header).put(inputString).flip();

        GSIEngineResult result = _testEngine.handshake(bigSrcBuffer,
                                                       smallDestBuffer);
        assertEquals(ResultStatus.BUFFER_OVERFLOW, result.getStatus());
    }

    @Test
    public void testGSISrcBufferUnderflow() throws GSSException, IOException {
        ByteBuffer incompleteSrcBuffer = ByteBuffer.allocate(6);
        ByteBuffer dummyDestBuffer = ByteBuffer.allocate(1);

        byte [] header = SimpleGSIEngineHelper.getGSIHeader(20);

        incompleteSrcBuffer.put(header).put((byte) 0).flip();

        GSIEngineResult result = _testEngine.handshake(incompleteSrcBuffer,
                                                       dummyDestBuffer);
        assertEquals(ResultStatus.BUFFER_UNDERFLOW, result.getStatus());
    }

    /**
     * Helper method - before unwrapping using a certain packet type (like
     * sslv3, sslv2 or GSI) the engine has to be in the right packet mode -
     * this can be achieved by having it unwrap a single packet with a
     * respective header.
     * @param header Packet header
     * @param inputString Some input string
     * @throws GSSException
     * @throws IOException
     */
    private void prepareEngineForUnwrap(byte [] header, byte [] inputString)
        throws GSSException, IOException {
        ByteBuffer src = ByteBuffer.allocate(header.length + inputString.length);
        /* 100% overhead should be enough */
        int tmpDestSize = Math.max(TCP_BUFFER_SIZE,
                                   (header.length + inputString.length)*2);

        ByteBuffer tmpDest = ByteBuffer.allocate(tmpDestSize);

        src.put(header).put(inputString).flip();
        src.mark();

        /* perform a fake handshake */
        int maxHandshakeIterations = MAX_HANDSHAKE_ITERATIONS;
        GSIEngineResult result;

        while (_testEngine.getHandshakeStatus() != HandshakeStatus.CH_ESTABLISHED) {
            result = _testEngine.handshake(src, tmpDest);
            src.reset();

            if (--maxHandshakeIterations <= 0 || result.getStatus() != ResultStatus.OK) {
                fail("Could not complete the handshake!");
            }
        }

        src.reset();
        tmpDest.clear();

        /* provoke the engine to switch into the right mode */
        result = _testEngine.unwrap(src, tmpDest);

        src.clear();
        tmpDest.clear();

    }

    @Test
    public void testGSIWrapUnwrap() throws GSSException, IOException {
        ByteBuffer srcBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);
        ByteBuffer dstBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);

        byte [] inputString = "Hello world".getBytes();
        byte [] header = SimpleGSIEngineHelper.getGSIHeader(inputString.length);

        /* get the engine into GSI mode */
        prepareEngineForUnwrap(header, inputString);

        /* no header, should be added by wrap */
        srcBuffer.put(inputString).flip();
        /* now let's go and wrap the information */
        GSIEngineResult result = _testEngine.wrap(srcBuffer, dstBuffer);
        dstBuffer.flip();
        srcBuffer.clear();

        result = _testEngine.unwrap(dstBuffer, srcBuffer);
        srcBuffer.flip();
        byte [] unwrapResult = new byte[result.getBytesProduced()];
        srcBuffer.get(unwrapResult);

        assertArrayEquals(inputString, unwrapResult);
    }

    @Test
    public void testSSLv2WrapUnwrap() throws GSSException, IOException {
        ByteBuffer srcBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);
        ByteBuffer dstBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);

        byte [] inputString = "Hello world".getBytes();
        byte [] header = SimpleGSIEngineHelper.getSSLV2Header(inputString.length);

        /* get the engine into SSLv2 mode */
        prepareEngineForUnwrap(header, inputString);

        srcBuffer.put(inputString).flip();
        GSIEngineResult result = _testEngine.wrap(srcBuffer, dstBuffer);
        dstBuffer.flip();
        srcBuffer.clear();

        result = _testEngine.unwrap(dstBuffer, srcBuffer);
        srcBuffer.flip();

        byte[] unwrapResult = new byte[result.getBytesProduced()];
        srcBuffer.get(unwrapResult);

        assertArrayEquals(inputString, unwrapResult);
    }

    @Test(expected=IOException.class)
    public void testTooLargeSSLV3Packet() throws GSSException, IOException {
        byte [] input = new byte[SSLV3_PAYLOAD_MAX_SIZE + 1];
        byte[] header = SimpleGSIEngineHelper.getSSLV3Header(input.length);

        prepareEngineForUnwrap(header, input);
    }

    /* can not really test too large packages for sslv2 - the header size
     * information is only 14 bits, so any attempt to send a too large packet
     * will just cause an overflow in the header. Also, the 14 bits are
     * unsigned, so it is not possible to send a negative header size. */

    @Test(expected=IOException.class)
    public void testTooLargeGSIPacket() throws GSSException, IOException {
        byte[] input = new byte[GSI_PAYLOAD_MAX_SIZE + 1];
        byte [] header = SimpleGSIEngineHelper.getGSIHeader(input.length);

        prepareEngineForUnwrap(header, input);
    }

    @Test(expected=IOException.class)
    public void testInvalidSSLV3Packet() throws GSSException, IOException {
        byte [] input = new byte[1];
        byte [] header = SimpleGSIEngineHelper.getSSLV3Header(-1);

        prepareEngineForUnwrap(header, input);
    }

    @Test(expected=IOException.class)
    public void testInvalidGSIPacket() throws GSSException, IOException {
        byte [] input = new byte[1];
        byte [] header = SimpleGSIEngineHelper.getGSIHeader(-1);

        prepareEngineForUnwrap(header, input);
    }

    @Test
    public void testClose() throws GSSException, IOException {
        ByteBuffer srcBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);
        ByteBuffer dstBuffer = ByteBuffer.allocate(TCP_BUFFER_SIZE);

        byte [] inputString = "Hello world".getBytes();
        byte [] header = SimpleGSIEngineHelper.getSSLV3Header((short) inputString.length);

        srcBuffer.put(header).put(inputString).flip();
        /* make sure that we have handshaken */
        prepareEngineForUnwrap(header, inputString);

        _testEngine.close();
        GSIEngineResult result = _testEngine.unwrap(srcBuffer, dstBuffer);

        assertEquals(ResultStatus.CLOSED, result.getStatus());
        assertEquals(HandshakeStatus.CH_CLOSED, _testEngine.getHandshakeStatus());
    }





}
