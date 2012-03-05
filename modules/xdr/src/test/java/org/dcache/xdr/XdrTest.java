package org.dcache.xdr;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.memory.MemoryManager;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class XdrTest {

    private Buffer _buffer;


    @Before
    public void setUp() {
        _buffer = MemoryManager.DEFAULT_MEMORY_MANAGER.allocate(1024);
        _buffer.order(ByteOrder.BIG_ENDIAN);
    }

    @Test
    public void testDecodeInt() {

        int value = 17;
        _buffer.putInt(value);

        Xdr xdr = new Xdr(_buffer);
        xdr.beginDecoding();

        assertEquals("Decode value incorrect", 17, xdr.xdrDecodeInt());
    }

    @Test
    public void testEncodeDecodeOpaque() {

        byte[] data = "some random data".getBytes();
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeDynamicOpaque(data);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        byte[] decoded = decoder.xdrDecodeDynamicOpaque();

        assertTrue("encoded/decoded data do not match", Arrays.equals(data, decoded));
    }


    @Test
    public void testDecodeBooleanTrue() {

        _buffer.putInt(1);

        Xdr xdr = new Xdr(_buffer);
        xdr.beginDecoding();
        assertTrue("Decoded value incorrect", xdr.xdrDecodeBoolean() );
    }

    @Test
    public void testEncodeDecodeBooleanTrue() {

        boolean value = true;
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeBoolean(value);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        boolean decoded = decoder.xdrDecodeBoolean();
        assertEquals("Decoded boolean value incorrect", value, decoded );
    }

    @Test
    public void testEncodeDecodeBooleanFalse() {

        boolean value = false;
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeBoolean(value);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        boolean decoded = decoder.xdrDecodeBoolean();
        assertEquals("Decoded boolean value incorrect", value, decoded );
    }

    @Test
    public void testDecodeBooleanFale() {

        _buffer.putInt(0);

        Xdr xdr = new Xdr(_buffer);
        xdr.beginDecoding();
        assertFalse("Decoded value incorrect", xdr.xdrDecodeBoolean() );
    }


    @Test
    public void testEncodeDecodeString() {

        String original = "some random data";
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeString(original);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        String decoded = decoder.xdrDecodeString();

        assertEquals("encoded/decoded string do not match", original, decoded);
    }

    @Test
    public void testEncodeDecodeEmptyString() {

        String original = "";
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeString(original);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        String decoded = decoder.xdrDecodeString();

        assertEquals("encoded/decoded string do not match", original, decoded);
    }

    @Test
    public void testEncodeDecodeNullString() {

        String original = null;
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeString(original);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        String decoded = decoder.xdrDecodeString();

        assertEquals("encoded/decoded string do not match", "", decoded);
    }

    @Test
    public void testEncodeDecodeLong() {

        long value = 7 << 32;
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeLong(value);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        long decoded = decoder.xdrDecodeLong();

        assertEquals("encoded/decoded long do not match", value, decoded);
    }

    @Test
    public void testEncodeDecodeMaxLong() {

        long value = Long.MAX_VALUE;
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeLong(value);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        long decoded = decoder.xdrDecodeLong();

        assertEquals("encoded/decoded long do not match", value, decoded);
    }

    @Test
    public void testEncodeDecodeMinLong() {

        long value = Long.MIN_VALUE;
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeLong(value);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        long decoded = decoder.xdrDecodeLong();

        assertEquals("encoded/decoded long do not match", value, decoded);
    }

    @Test
    public void testEncodeDecodeIntVector() {

        int vector[] = { 1, 2, 3, 4 };
        XdrEncodingStream encoder = new Xdr(_buffer);
        encoder.beginEncoding();
        encoder.xdrEncodeIntVector(vector);
        encoder.endEncoding();

        XdrDecodingStream decoder = new Xdr(_buffer);
        decoder.beginDecoding();

        int[] decoded = decoder.xdrDecodeIntVector();

        assertArrayEquals("encoded/decoded int array do not match", vector, decoded);
    }

    @Test
    public void testSizeConstructor() {

        Xdr xdr = new Xdr(1024);

        assertEquals("encode/decode buffer size mismatch", 1024, xdr.body().capacity());
    }
}
