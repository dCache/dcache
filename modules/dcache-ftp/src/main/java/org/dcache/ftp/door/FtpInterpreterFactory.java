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
package org.dcache.ftp.door;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import diskCacheV111.doors.LineBasedInterpreter;
import diskCacheV111.doors.LineBasedInterpreterFactory;

import dmg.cells.nucleus.CellEndpoint;
import dmg.util.StreamEngine;

import org.dcache.util.Args;
import org.dcache.util.OptionParser;

public abstract class FtpInterpreterFactory implements LineBasedInterpreterFactory
{
    private OptionParser options;

    protected abstract AbstractFtpDoorV1 createInterpreter();

    @Override
    public void configure(Args args)
    {
        options = new OptionParser(args);
    }

    @Override
    public LineBasedInterpreter create(CellEndpoint endpoint, StreamEngine engine, Executor executor) throws Exception
    {
        AbstractFtpDoorV1 interpreter = options.inject(createInterpreter());
        interpreter.setWriter(engine.getWriter());
        interpreter.setRemoteSocketAddress((InetSocketAddress) engine.getSocket().getRemoteSocketAddress());
        interpreter.setLocalSocketAddress((InetSocketAddress) engine.getSocket().getLocalSocketAddress());
        interpreter.setExecutor(executor);
        interpreter.setCellEndpoint(endpoint);
        interpreter.init();
        return interpreter;
    }
}
