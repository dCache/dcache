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

import dmg.util.CommandExitException;

/**
 * Protocol interpreter to be used with a {@link LineBasedDoor}.
 *
 * <p>Suitable for line oriented protocols like FTP and DCAP. The methods of this interface are
 * called sequentially.
 *
 * <p>Implementations may optionally implement {@link dmg.cells.nucleus.CellCommandListener},
 * {@link dmg.cells.nucleus.CellMessageReceiver} and {@link dmg.cells.nucleus.CellInfoProvider}
 * to participate in other operations of the door. The methods of those interface will be called
 * concurrently with the methods of this interface.
 *
 * @see LineBasedInterpreterFactory
 */
public interface LineBasedInterpreter
{
    /**
     * Process an command line read from the client.
     *
     * @param cmd
     * @throws CommandExitException To terminate the connection to the door.
     */
    void execute(String cmd) throws CommandExitException;

    /**
     * Signals that the connection with the client is being terminated.
     */
    void shutdown();
}
