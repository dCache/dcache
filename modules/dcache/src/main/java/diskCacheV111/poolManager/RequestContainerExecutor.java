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
package diskCacheV111.poolManager;

import java.io.PrintWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.command.Argument;
import dmg.util.command.Command;

public class RequestContainerExecutor
        extends ThreadPoolExecutor
        implements CellCommandListener, CellSetupProvider
{
    public RequestContainerExecutor()
    {
        super(Runtime.getRuntime().availableProcessors(), Integer.MAX_VALUE,
              60L, TimeUnit.SECONDS,
              new LinkedBlockingQueue<Runnable>(128));
    }

    @Command(name = "rc set max threads", hint = "set request container thread limit",
             description = "Sets the maximum number of requests allocated for " +
                     "processing read requests.")
    class SetMaxThreadsCommand implements Callable<String>
    {
        @Argument
        int count;

        @Override
        public String call() throws Exception
        {
            if (count == 0) {
                count = Integer.MAX_VALUE;
            }
            setMaximumPoolSize(count);
            return "New max thread count : " + count;
        }
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        pw.append("rc set max threads ").println(getMaximumPoolSize());
    }

    @Override
    public void beforeSetup()
    {
    }

    @Override
    public void afterSetup()
    {
    }
}
