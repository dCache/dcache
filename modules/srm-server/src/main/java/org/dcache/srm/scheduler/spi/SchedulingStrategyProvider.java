package org.dcache.srm.scheduler.spi;

import java.util.Map;

import org.dcache.srm.scheduler.Scheduler;

/**
 * Service provider interface to instantiate implementations of SchedulingStrategy.
 *
 * Implementations are discovered using Java's ServiceLoader mechanism.
 */
public interface SchedulingStrategyProvider
{
    /**
     * The name an admin uses to identify the strategy.
     */
    String getName();

    /**
     * Sets configuration parameters for the strategy.
     */
    void setConfiguration(Map<String,String> configuration);

    /**
     * Creates a new scheduling strategy for a given scheduler.
     */
    SchedulingStrategy createStrategy(Scheduler scheduler);
}
