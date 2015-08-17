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

import java.util.Map;

import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.spi.SchedulingStrategy;
import org.dcache.srm.scheduler.spi.SchedulingStrategyProvider;

public class InProgressFairShareSchedulingStrategyProvider implements SchedulingStrategyProvider
{
    private String discriminator;

    @Override
    public String getName()
    {
        return "inprogress-fair-share";
    }

    @Override
    public void setConfiguration(Map<String, String> configuration)
    {
        discriminator = configuration.get("discriminator");
    }

    @Override
    public SchedulingStrategy createStrategy(Scheduler scheduler)
    {
        return new InProgressFairShareSchedulingStrategy(scheduler, discriminator);
    }
}
