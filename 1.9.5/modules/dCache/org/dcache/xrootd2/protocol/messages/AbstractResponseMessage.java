package org.dcache.xrootd2.protocol.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

import java.io.UnsupportedEncodingException;

public abstract class AbstractResponseMessage
{
    protected ChannelBuffer _buffer;

    public AbstractResponseMessage(int sId, int stat, int length)
    {
        _buffer = ChannelBuffers.dynamicBuffer(SERVER_RESPONSE_LEN + length);

        putUnsignedShort(sId);
        putUnsignedShort(stat);


        // The following field is the length of the payload. We set it
        // to zero as the exact length is not known yet. The
        // XrootdDecoder will fill in the correct value before putting
        // the response on the wire.
        putSignedInt(0);
    }

    public AbstractResponseMessage(int sId, int stat)
    {
        putUnsignedShort(sId);
        putUnsignedShort(stat);
    }

    public void setStatus(int s)
    {
        _buffer.setByte(2, (byte) (s >> 8));
        _buffer.setByte(3, (byte) s);
    }

    protected void put(byte[] field)
    {
        _buffer.writeBytes(field);
    }

    protected void putUnsignedChar(int c)
    {
        _buffer.writeByte((byte) c);
    }

    protected void putUnsignedShort(int s)
    {
        _buffer.writeByte((byte) (s >> 8));
        _buffer.writeByte((byte) s);
    }

    protected void putSignedInt(int i)
    {
        _buffer.writeInt(i);
    }

    protected void putSignedLong(long l)
    {
        _buffer.writeLong(l);
    }

    /**
     * Put all characters of a String as unsigned kXR_chars
     * @param s the String representing the char sequence to put
     */
    protected void putCharSequence(String s)
    {
        try {
            put(s.getBytes("ASCII"));
        } catch (UnsupportedEncodingException e) {
            /* We cannot possibly recover from this option, so
             * escalate it.
             */
            throw new RuntimeException("Failed to construct xrootd message", e);
        }
    }

    /**
     * Gives access to the internal ChannelBuffer of the response. The
     * response object is no longer valid if the read index of the
     * buffer is changed.
     */
    public ChannelBuffer getBuffer()
    {
        return _buffer;
    }
}
