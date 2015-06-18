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

import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.Request;
import org.dcache.srm.scheduler.spi.JobDiscriminator;

public abstract class UserDiscriminator implements JobDiscriminator
{
    @Nonnull
    @Override
    public String getDiscriminatingValue(Job job)
    {
        SRMUser user = getUser(job);
        return (user == null) ? "" : getDiscriminatingValue(user);
    }

    private SRMUser getUser(Job job)
    {
        if (job instanceof Request) {
            return ((Request) job).getUser();
        } else if (job instanceof FileRequest) {
            try {
                return ((FileRequest) job).getUser();
            } catch (SRMInvalidRequestException ignored) {
            }
        }
        return null;
    }

    @Nonnull
    protected abstract String getDiscriminatingValue(SRMUser user);
}
