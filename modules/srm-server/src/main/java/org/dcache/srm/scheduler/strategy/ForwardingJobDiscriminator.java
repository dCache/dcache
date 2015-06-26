/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.srm.scheduler.strategy;

import javax.annotation.Nonnull;

import java.util.ServiceLoader;

import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.spi.JobDiscriminator;

public class ForwardingJobDiscriminator implements JobDiscriminator
{
    private final ServiceLoader<JobDiscriminator> discriminators =
            ServiceLoader.load(JobDiscriminator.class);
    protected final JobDiscriminator discriminator;

    public ForwardingJobDiscriminator(String key)
    {
        this.discriminator = getDiscriminator(key);
    }

    @Nonnull
    @Override
    public String getKey()
    {
        return discriminator.getKey();
    }

    @Nonnull
    @Override
    public String getDiscriminatingValue(Job job)
    {
        return discriminator.getDiscriminatingValue(job);
    }

    protected JobDiscriminator getDiscriminator(String key)
    {
        for (JobDiscriminator discriminator : discriminators) {
            if (discriminator.getKey().equals(key)) {
                return discriminator;
            }
        }
        throw new IllegalArgumentException("No such job discriminator: " + key);
    }
}
