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
import org.dcache.srm.scheduler.spi.TransferStrategy;
import org.dcache.srm.scheduler.spi.TransferStrategyProvider;

public class FirstComeFirstServedTransferStrategyProvider implements TransferStrategyProvider
{
    @Override
    public String getName()
    {
        return "first-come-first-served";
    }

    @Override
    public void setConfiguration(Map<String, String> configuration)
    {
    }

    @Override
    public TransferStrategy createStrategy(Scheduler scheduler)
    {
        return new FirstComeFirstServedTransferStrategy(scheduler);
    }
}
