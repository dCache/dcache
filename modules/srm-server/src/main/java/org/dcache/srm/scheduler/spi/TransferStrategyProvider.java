package org.dcache.srm.scheduler.spi;

import java.util.Map;

import org.dcache.srm.scheduler.Scheduler;

/**
 * Service provider interface to instantiate implementations of TransferStrategy.
 *
 * Implementations are discovered using Java's ServiceLoader mechanism.
 */
public interface TransferStrategyProvider
{
    /**
     * The name an admin uses to identify the strategy.
     */
    String getName();

    /**
     * Sets configuration parameters for the strategy.
     */
    void setConfiguration(Map<String, String> configuration);

    /**
     * Creates a new transfer strategy for a given scheduler.
     */
    TransferStrategy createStrategy(Scheduler scheduler);
}
