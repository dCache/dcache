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
package org.dcache.srm.taperecallscheduling;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandException;
import dmg.util.command.Command;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.dcache.srm.taperecallscheduling.spi.TapeInfoProvider;
import org.dcache.srm.taperecallscheduling.spi.TapeInfoProviderProvider;
import org.springframework.beans.factory.annotation.Required;

public class TapeInformant implements CellMessageReceiver, CellInfoProvider, CellCommandListener {

    private TapeInfoProvider tapeInfoProvider;

    @Required
    public void setTapeInfoProviderProvider(TapeInfoProviderProvider provider) {
        setTapeInfoProvider(provider.createProvider());
    }

    public void setTapeInfoProvider(TapeInfoProvider provider) {
        tapeInfoProvider = provider;
    }

    public Map<String, TapeInfo> getTapeInfos(List<String> tapes) {
        return tapeInfoProvider.getTapeInfos(tapes);
    }

    public Map<String, TapefileInfo> getTapefileInfos(List<String> fileids) {
        return tapeInfoProvider.getTapefileInfos(fileids);
    }

    @Command(name = "trs reload tape info",
          hint = "The tape recall scheduler reloads the tape location information files on the next run")
    public class ReloadTapeInfoCommand implements Callable<String> {

        @Override
        public synchronized String call()
              throws InterruptedException, NoRouteToCellException, CommandException {
            tapeInfoProvider.reload();
            return "Tape information will be reloaded during the next run";
        }
    }

    @Override
    public synchronized void getInfo(PrintWriter pw) {
        pw.printf(tapeInfoProvider.describe());
    }

}
