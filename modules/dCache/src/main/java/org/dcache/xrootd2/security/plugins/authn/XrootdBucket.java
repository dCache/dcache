package org.dcache.xrootd2.security.plugins.authn;

import java.io.IOException;
import java.io.OutputStream;
import org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.BucketType;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * An XrootdBucket is a serialized datatype (string, uint, binary, list) with
 * an int32 header describing its contents. The headers are well defined and
 * for each header it is known which datatype to expect.
 *
 *
 * @author radicke
 * @author tzangerl
 *
 */
public abstract class XrootdBucket
{

    protected BucketType _type;

    public XrootdBucket(BucketType type) {
        _type = type;
    }

    public BucketType getType() {
        return _type;
    }

    public void serialize(ChannelBuffer out) {
        out.writeInt(_type.getCode());
    }

    /**
     * Deserialize an XrootdBucket. Depending on the BucketType, return an
     * XrootdBucket of a specific subtype.
     *
     * The only type where the returned type is not a-priori known is
     * kXRS_main, which can be encrypted. If it is encrypted, a binary (raw)
     * bucket is returned, if it is not encyrpted, a list of contained
     * buckets (nestedBuffer) is returned.
     *
     * @param type The type of the bucket that should be deserialized
     * @param buffer The buffer containing the buckets
     * @return The deserialized bucket
     * @throws IOException Deserialization fails.
     */
    public static XrootdBucket deserialize(BucketType type, ChannelBuffer buffer)
        throws IOException {

        XrootdBucket bucket;

        switch (type) {

            case kXRS_main:

                try {

                    bucket = NestedBucketBuffer.deserialize(type, buffer);

                } catch (IOException e) {
                    // ok the nested buffer seems to be encrypted
                    // just store the binary data for now, it will be decrypted later on
                    bucket = RawBucket.deserialize(type, buffer);
                }

                break;

            case kXRS_cryptomod:    // fall through
            case kXRS_issuer_hash:  // fall through
            case kXRS_rtag:         // fall through
            case kXRS_puk:          // fall through
            case kXRS_cipher_alg:   // fall through
            case kXRS_x509:         // fall through
            case kXRS_md_alg:

                bucket = StringBucket.deserialize(type, buffer);
                break;

            case kXRS_version:      // fall through
            case kXRS_clnt_opts:

                bucket = UnsignedIntBucket.deserialize(type, buffer);
                break;

            default:

                bucket = RawBucket.deserialize(type, buffer);
                break;
        }

        return bucket;
    }

    /**
     * @return Length of the serialized bucket (in bytes)
     */
    public int getSize() {
        return 4;
    }

    @Override
    public String toString() {
        return "bucket type: "+ _type +"\n";
    }

    protected void writeInt(OutputStream out, int v) throws IOException {
        out.write( (byte) (v >> 24));
        out.write( (byte) (v >> 16));
        out.write( (byte) (v >> 8));
        out.write( (byte)  v);
    }
}

