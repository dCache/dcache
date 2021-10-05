/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
import org.dcache.srm.taperecallscheduling.TapeInformant;
import org.dcache.srm.taperecallscheduling.TapeRecallSchedulingRequirementsChecker;

public class TapeRecallSchedulingStrategyProvider implements SchedulingStrategyProvider {

    private TapeInformant tapeInformant;
    private TapeRecallSchedulingRequirementsChecker requirementsChecker;


    @Override
    public String getName() {
        return "tape-recall-scheduling";
    }

    @Override
    public void setConfiguration(Map<String, String> configuration) {
    }

    public void setTapeInformant(TapeInformant informant) {
        tapeInformant = informant;
    }

    public void setRequirementsChecker(TapeRecallSchedulingRequirementsChecker checker) {
        this.requirementsChecker = checker;
    }

    @Override
    public SchedulingStrategy createStrategy(Scheduler scheduler) {
        TapeRecallSchedulingStrategy strategy = new TapeRecallSchedulingStrategy();
        strategy.setTapeInformant(tapeInformant);
        strategy.setRequirementsChecker(requirementsChecker);
        return strategy;
    }
}
