package org.dcache.services.info.gathers;

/**
 * A Class that implements MessageMetadataRepository provides the ability to
 * store some simple metadata against an outgoing message. This provides the
 * facility to extract metadata when processing the reply message.
 */
public interface MessageMetadataRepository<ID>
{
    /**
     * Recored a TTL against an outgoing message.
     *
     * @param messageId
     *            the ID of this outgoing message.
     * @param ttl
     *            the duration, in seconds, for metrics derived from this
     *            message.
     */
    void putMetricTTL(ID messageId, long ttl);

    /**
     * Look up the recorded TTL against an outgoing message.
     *
     * @param messageId
     *            the ID of the outgoing message.
     * @return the recorded TTL, in seconds, for this outgoing message.
     * @throws IllegalArgumentException
     *             if no TTL was recorded for this messageID
     */
    long getMetricTTL(ID messageId);

    /**
     * Remove all metadata associated with a message.
     *
     * @param messageId
     *            the message about which all metadata will be purged.
     */
    void remove(ID messageId);

    /**
     * Query whether a TTL was recorded against this message ID.
     *
     * @param messageId
     *            the ID of the outgoing message.
     * @returns true if a TTL was recorded against the message ID, false
     *          otherwise.
     */
    boolean containsMetricTTL(ID messageId);
}
