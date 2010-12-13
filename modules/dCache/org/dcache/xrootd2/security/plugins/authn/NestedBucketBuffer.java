package org.dcache.xrootd2.security.plugins.authn;

import java.io.IOException;

import java.util.Map;

import org.dcache.xrootd2.protocol.messages.AuthenticationRequest;
import org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.BucketType;
import static org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.*;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Format of a NestedBucketBuffer:
 *
 *  - int32 BucketType (usually kXRS_main)
 *  - int32 len
 *  - char[4] protocol (\0 padded)
 *  - int32 step (e.g. kXGS_cert)
 *
 *      -- int32 BucketType (first nested bucket)
 *      -- int32 len
 *      -- byte[len] bucket-content
 *      -- int32 kXRS_none
 *
 *      -- int32 BucketType (second nested bucket)
 *      ...
 *
 *  - kXRS_none
 *
 * @see XrootdBucket
 *
 * @author radicke
 * @author tzangerl
 *
 */
public class NestedBucketBuffer extends XrootdBucket {
    private final static Logger _logger =
        LoggerFactory.getLogger(NestedBucketBuffer.class);
    private Map<BucketType, XrootdBucket> _nestedBuckets;
    private String _protocol;
    private int _step;

    public NestedBucketBuffer(BucketType type,
                              String protocol,
                              int step,
                              Map<BucketType, XrootdBucket> nestedBuckets) {
        super(type);
        _protocol = protocol;
        _step = step;
        _nestedBuckets = nestedBuckets;
    }

    /**
     * Deserialize the NestedBucketBuffer. Retrieve all the buckets and
     * recursively deserialize them. Also, retrieve the protocol information
     * and the step.
     *
     * @param type The type of the bucket (usually kXRS_main)
     * @param buffer The buffer containing the nested bucket buffer
     * @return Deserialized buffer
     * @throws IOException Deserialization fails
     */
    public static NestedBucketBuffer deserialize(BucketType type, ChannelBuffer buffer)
        throws IOException {

        /* kXRS_main can be a nested or an encrypted (raw) bucket. Try whether it
         * looks like a nested buffer and use raw deserialization if not */
        int readIndex = buffer.readerIndex();

        String protocol = AuthenticationRequest.deserializeProtocol(buffer);

        int step = AuthenticationRequest.deserializeStep(buffer);

        _logger.info("NestedBucketBuffer protocol: {}, step {}", protocol, step);

        if (step < kXGC_certreq || step > kXGC_reserved) {
            /* reset buffer */
            buffer.readerIndex(readIndex);
            throw new IOException("Buffer contents are not a nested buffer!");
        }

        NestedBucketBuffer bucket = new NestedBucketBuffer(type,
                                                           protocol,
                                                           step,
                                                           AuthenticationRequest.deserializeBuckets(buffer));

        return bucket;
    }

    /**
     *
     * @return the list of XrootdBuckets nested in this buffer
     */
    public Map<BucketType, XrootdBucket> getNestedBuckets() {
        return _nestedBuckets;
    }

    public int getStep() {
        return _step;
    }

    public String getProtocol() {
        return _protocol;
    }

    @Override
    /**
     * Serialize all the buckets in that buffer to an outputstream.
     *
     * @param out The ChannelBuffer to which this buffer will be serialized
     */
    public void serialize(ChannelBuffer out) {

        super.serialize(out);

        //
        // The nesting is a bit tricky. First, we skip 4 bytes (here we store later the
        // size of the nested serialized bucket buffer, which we don't know yet). Then, we
        // serialize the nested bucket buffer and store it in the bytebuffer. Then we jump
        // back to the previously marked position and store the size of the nested bucket buffer.
        //
        int currentpos = out.writerIndex();
        out.writeInt(0); // placeholder value

        out.writeBytes(_protocol.getBytes());

        /* the protocol must be 0-padded to 4 bytes */
        int padding = 4 - _protocol.getBytes().length;

        for (int i=0; i < padding; i++) {
            out.writeByte(0);
        }

        out.writeInt(_step);

        for (XrootdBucket bucket : _nestedBuckets.values()) {
            bucket.serialize(out);
        }

        out.writeInt(BucketType.kXRS_none.getCode());

        int nestedBucketBufferLength = out.writerIndex() - currentpos - 4;
        out.writerIndex(currentpos);
        out.writeInt(nestedBucketBufferLength);

        // place the position cursor at the end of the stored data to allow a clean flip()
        out.writerIndex(out.writerIndex()+nestedBucketBufferLength);
    }

    @Override
    public int getSize() {
        int size = super.getSize();

        for (XrootdBucket bucket : _nestedBuckets.values()) {
            size += bucket.getSize();
        }

        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString()).append("begin nested BucketBuffer\n");

        for (XrootdBucket bucket : _nestedBuckets.values()) {
            sb.append(bucket.toString());
        }

        sb.append("end nested BucketBuffer\n");

        return sb.toString();
    }
}
