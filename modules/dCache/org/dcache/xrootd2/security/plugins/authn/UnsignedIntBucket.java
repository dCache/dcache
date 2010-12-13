package org.dcache.xrootd2.security.plugins.authn;

import org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.BucketType;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * A bucket containing a header plus an unsigned integer.
 *
 * @see XrootdBucket
 *
 * @author radicke
 * @author tzangerl
 *
 */
public class UnsignedIntBucket extends XrootdBucket
{
   private int _data;

    public UnsignedIntBucket(BucketType type, int data) {
        super(type);
        _data = data;
    }

    public int getContent() {
        return _data;
    }

    public static UnsignedIntBucket deserialize(BucketType type, ChannelBuffer buffer) {

        UnsignedIntBucket bucket = new UnsignedIntBucket(type, buffer.getInt(0));

        return bucket;
    }

    @Override
    public void serialize(ChannelBuffer out) {
        super.serialize(out);
        out.writeInt(4);
        out.writeInt(_data);
    }

    @Override
    public int getSize() {
        return super.getSize() + 8;
    }

    @Override
    public String toString() {
        return super.toString() + " decimal int: "+ _data;
    }
}
