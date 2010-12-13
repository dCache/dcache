package org.dcache.xrootd2.protocol.messages;

import java.util.List;

import org.dcache.xrootd2.security.plugins.authn.XrootdBucket;
import org.dcache.xrootd2.security.plugins.authn.XrootdSecurityProtocol.BucketType;

public class AuthenticationResponse extends AbstractResponseMessage
{
    /**
     * Default authentication response, usually sent finally, if all previous
     * steps are okay
     * @param sId
     * @param status
     * @param length
     */
    public AuthenticationResponse(int sId, int status, int length)
    {
        super(sId, status, length);
    }

    /**
     * Intermediate AuthenticationResponse.
     *
     * @param sId the streamID, matching the request
     * @param status the status (usually kXR_authmore)
     * @param length
     * @param protocol the currently used authentication protocol
     * @param step the processing step
     * @param buckets list of buckets containing server-side authentication
     *                information (challenge, host certificate, etc.)
     */
    public AuthenticationResponse(int sId,
                                  int status,
                                  int length,
                                  String protocol,
                                  int step,
                                  List<XrootdBucket> buckets) {
        super(sId, status, length);

        if (protocol.length() > 4) {
            throw new IllegalArgumentException("Protocol length must not " +
                                               "exceed 4. The passed protocol is "
                                               + protocol);
        }

        putCharSequence(protocol);

        /* the protocol must be 0-padded to 4 bytes */
        int padding = 4 - protocol.getBytes().length;

        for (int i=0; i < padding; i++) {
            _buffer.writeByte(0);
        }

        putSignedInt(step);

        for (XrootdBucket bucket : buckets) {
            bucket.serialize(_buffer);
        }

        putSignedInt(BucketType.kXRS_none.getCode());
    }

}
