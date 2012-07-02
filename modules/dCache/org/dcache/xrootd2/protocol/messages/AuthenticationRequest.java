package org.dcache.xrootd2.protocol.messages;
import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.EnumMap;
import java.util.Map;

import org.dcache.xrootd2.security.plugins.authn.XrootdBucket;
import org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.BucketType;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationRequest extends AbstractRequestMessage
{
    private final static Logger _logger =
        LoggerFactory.getLogger(AuthenticationRequest.class);

    /** the protocol as it is send by the client, zero-padded char[4] */
    private String _protocol;
    /** the step as it is send by the client, int32 */
    private int _step;
    /** store the buckets (kind of a serialized datatype with an
     * int32 block of metadata) received from the client
     */
    private Map<BucketType, XrootdBucket> _bucketMap =
        new EnumMap<BucketType, XrootdBucket>(BucketType.class);

    /**
     * Deserialize protocol, processing step and all the bucks sent by the
     * client
     * @param buffer The buffer containing the above
     */
    public AuthenticationRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != kXR_auth) {
            throw new IllegalArgumentException("doesn't seem to be a kXR_auth message");
        }

        /* skip reserved bytes and credlen */
        buffer.readerIndex(24);

        _protocol = deserializeProtocol(buffer);
        _step = deserializeStep(buffer);

        try {
            _bucketMap.putAll(deserializeBuckets(buffer));
        } catch (IOException ioex) {
            throw new IllegalArgumentException("Illegal credential format: {}",
                                               ioex);
        }
    }

    /**
     * Deserialize the buckets sent by the client and put them into a EnumMap
     * sorted by their header-information. As there are list-type buffers,
     * this method can be called recursively. In current xrootd, this is
     * limited to a maximum of 1 recursion (main buffer containing list of
     * further buffers).
     *
     * @param buffer The buffer containing the received buckets
     * @return Map from bucket-type to deserialized buckets
     * @throws IOException Failure of deserialization
     */
    public static Map<BucketType, XrootdBucket> deserializeBuckets(ChannelBuffer buffer)
        throws IOException {

        int bucketCode = buffer.readInt();
        BucketType bucketType = BucketType.get(bucketCode);

        _logger.debug("Deserializing a bucket with code {}", bucketCode);

        Map<BucketType, XrootdBucket> buckets =
            new EnumMap<BucketType,XrootdBucket>(BucketType.class);

        while (bucketType != BucketType.kXRS_none) {
            int bucketLength = buffer.readInt();

            XrootdBucket bucket = XrootdBucket.deserialize(bucketType,
                                                           buffer.slice(buffer.readerIndex(), bucketLength));
            buckets.put(bucketType, bucket);

            /* proceed to the next bucket */
            buffer.readerIndex(buffer.readerIndex() + bucketLength);

            bucketCode = buffer.readInt();
            bucketType = BucketType.get(bucketCode);
        }

        return buckets;
    }

    public static String deserializeProtocol(ChannelBuffer buffer) {
       String protocol = buffer.toString(buffer.readerIndex(),
                                         4,
                                         Charset.forName("ASCII")).trim();

       /* toString does not advance the index */
       buffer.readerIndex(buffer.readerIndex() + 4);
       return protocol;
    }

    public static int deserializeStep(ChannelBuffer buffer) {
        int step = buffer.readInt();
        return step;
    }

    public Map<BucketType, XrootdBucket> getBuckets() {
        return _bucketMap;
    }

    public int getStep() {
        return _step;
    }

    public String getProtocol() {
        return _protocol;
    }
}
