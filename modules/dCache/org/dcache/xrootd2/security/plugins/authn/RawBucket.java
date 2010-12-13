package org.dcache.xrootd2.security.plugins.authn;


import org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.BucketType;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * A bucket containing a header plus a number of bytes. This can be binary
 * data, but a raw-bucket can also represent encryptet buckets of another
 * type.
 *
 * @see XrootdBucket
 *
 * @author radicke
 * @author tzangerl
 *
 */
public class RawBucket extends XrootdBucket
{
    private byte[] _data;

    public RawBucket(BucketType type, byte[] data) {
        super(type);
        _data = data;
    }

    public byte[] getContent() {
        return _data;
    }

    public static RawBucket deserialize(BucketType type, ChannelBuffer buffer) {

        byte [] tmp = new byte[buffer.readableBytes()];
        buffer.getBytes(0, tmp);
        RawBucket bucket = new RawBucket(type, tmp);

        return bucket;
    }

    @Override
    public void serialize(ChannelBuffer out) {
        super.serialize(out);
        out.writeInt(_data.length);
        out.writeBytes(_data);
    }

    @Override
    public int getSize() {
        return super.getSize() + 4 + _data.length;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString()+" hex dump:");

        for (int i = 0; i < _data.length;i++) {
            sb.append(" "+Integer.toHexString(_data[i]));
        }

        return sb.toString();
    }
}

