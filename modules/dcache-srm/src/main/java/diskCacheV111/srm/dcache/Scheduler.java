/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.srm.dcache;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import java.io.PrintWriter;
import org.dcache.srm.request.Job;

/**
 * Simple sub-class of SRM Scheduler to implement the CellInfoProvider interface.
 */
public class Scheduler<T extends Job> extends org.dcache.srm.scheduler.Scheduler<T> implements
      CellInfoProvider {

    public Scheduler(String id, Class<T> type) {
        super(id, type);
    }

    @Override
    public void getInfo(PrintWriter pw) {
        StringBuilder sb = new StringBuilder();
        getInfo(sb);
        pw.append(sb);
    }

    @Override
    public CellInfo getCellInfo(CellInfo info) {
        return info;
    }
}
