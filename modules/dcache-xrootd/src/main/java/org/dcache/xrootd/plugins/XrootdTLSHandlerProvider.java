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
package org.dcache.xrootd.plugins;

import java.util.Properties;

public class XrootdTLSHandlerProvider implements ChannelHandlerProvider
{
    public static final String PLUGIN = "ssl-handler";
    public static final String CLIENT_PLUGIN = "ssl-client-handler";

    @Override
    public ChannelHandlerFactory createFactory(String plugin, Properties properties)
                    throws Exception
    {
        if (plugin.equals(PLUGIN)) {
            return new XrootdTLSHandlerFactory(properties, true);
        } else if (plugin.equals(CLIENT_PLUGIN)) {
            return new XrootdTLSHandlerFactory(properties, false);
        }

        return null;
    }
}
