package org.dcache.poolmanager;

import java.util.Map;

/**
 * Service Provider Interface representing a partition type.
 *
 * A Partition plugin must implement the PartitionFactory
 * interface. It is the primary means of creating new partitions of
 * the given partition type.
 *
 * The Java 6 ServiceLoader facility is used to locate and instantiate
 * the implementation of this interface. The implementation must be
 * packages in a JAR file containing the
 * META-INF/services/org.dcache.poolmanager.PartitionFactory file
 * containing the fully qualified class name of the implementation on
 * a single line.
 */
public interface PartitionFactory
{
    /**
     * Factory method. Creates a new Partition.
     *
     * The configuration parameter are shared parameters used by all
     * partitions. The parameters are not specific to this particular
     * instance.
     *
     * Instance specific configuration parameters are injected
     * afterwards by using a copy-on-write scheme using the create
     * method of the Partition.
     *
     * @param inherited Shared configuration parameters
     */
    Partition createPartition(Map<String,String> inherited);

    /**
     * Returns a short (one line) human readable description of the
     * partition type.
     */
    String getDescription();

    /**
     * Returns a string identifier of the partition type. This string
     * is used by the admin to indicate that the partition type
     * implemented by this PartitionFactory is to be used.
     */
    String getType();
}
