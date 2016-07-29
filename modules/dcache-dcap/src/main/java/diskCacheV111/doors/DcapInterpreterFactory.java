/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.doors;

import java.util.concurrent.Executor;

import diskCacheV111.util.ConfigurationException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.util.StreamEngine;

import org.dcache.util.Args;
import org.dcache.util.OptionParser;

/**
 * Factory to support DCAP with the {@link LineBasedDoor}.
 *
 * @see DcapLineBasedInterpreterAdapter
 */
public class DcapInterpreterFactory implements LineBasedInterpreterFactory
{
    protected final DcapDoorSettings settings = new DcapDoorSettings();

    @Override
    public void configure(Args args) throws ConfigurationException
    {
        new OptionParser(args).inject(settings).init();
    }

    @Override
    public LineBasedInterpreter create(CellEndpoint endpoint, CellAddressCore myAddress, StreamEngine engine,
                                       Executor executor)
    {
        return new DcapLineBasedInterpreterAdapter(endpoint, myAddress, engine, settings);
    }
}
