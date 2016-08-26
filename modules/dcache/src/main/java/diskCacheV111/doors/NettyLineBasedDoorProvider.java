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

import diskCacheV111.util.ConfigurationException;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.services.login.LoginCellFactory;
import dmg.cells.services.login.LoginCellProvider;

import org.dcache.util.Args;

public class NettyLineBasedDoorProvider implements LoginCellProvider
{
    @Override
    public int getPriority(String name)
    {
        try {
            if (NettyLineBasedInterpreterFactory.class.isAssignableFrom(Class.forName(name))) {
                return 100;
            }
        } catch (ClassNotFoundException ignored) {
        }
        return Integer.MIN_VALUE;
    }

    @Override
    public LoginCellFactory createFactory(String name, Args args, CellEndpoint parentEndpoint, String parentCellName)
            throws IllegalArgumentException
    {
        try {
            Class<?> interpreter = Class.forName(name);
            if (NettyLineBasedInterpreterFactory.class.isAssignableFrom(interpreter)) {
                NettyLineBasedInterpreterFactory factory = interpreter.asSubclass(NettyLineBasedInterpreterFactory.class).newInstance();
                factory.configure(args);
                return new NettyLineBasedDoorFactory(factory, args, parentEndpoint, parentCellName);
            }
            throw new IllegalArgumentException("Not a NettyLineBasedInterpreterFactory: " + interpreter);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | ConfigurationException e) {
            throw new IllegalArgumentException("Failed to instantiate interpreter factory: " + e.toString(), e);
        }
    }
}
