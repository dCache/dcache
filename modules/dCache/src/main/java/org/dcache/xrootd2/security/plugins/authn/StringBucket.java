package org.dcache.xrootd2.security.plugins.authn;

import java.nio.charset.Charset;

import org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.BucketType;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * A bucket containing a header plus a String.
 *
 * @see XrootdBucket
 *
 * @author radicke
 * @author tzangerl
 *
 */
public class StringBucket extends XrootdBucket {
  private String _data;

    public StringBucket(BucketType type, String data) {
        super(type);
        _data = data;
    }

    public String getContent() {
        return _data;
    }

    public static StringBucket deserialize(BucketType type, ChannelBuffer buffer) {

        String s = buffer.toString(Charset.forName("ASCII"));
        StringBucket bucket = new StringBucket(type, s);
        return bucket;
    }

    @Override
    public void serialize(ChannelBuffer out) {
        super.serialize(out);
        out.writeInt(_data.length());
        out.writeBytes(_data.getBytes());
    }

    @Override
    public int getSize() {
        return super.getSize() + 4 + _data.getBytes().length;
    }

    @Override
    public String toString() {
        return super.toString() + _data;
    }

}
