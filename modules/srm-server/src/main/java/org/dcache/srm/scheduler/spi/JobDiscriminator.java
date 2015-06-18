package org.dcache.srm.scheduler.spi;

import javax.annotation.Nonnull;

import org.dcache.srm.request.Job;

/**
 * A SPI for classes that can provide discriminating values for jobs. Typically used
 * by schedulers to group jobs.
 *
 * Implementations are discovered using Java's ServiceLoader mechanism.
 */
public interface JobDiscriminator
{
    /**
     * Given a job return a discriminating value.
     */
    @Nonnull
    String getDiscriminatingValue(Job job);

    /**
     * The key under which the discriminating value should be identified.
     */
    @Nonnull
    String getKey();
}
