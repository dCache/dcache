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

import io.netty.channel.ChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessLogHandlerFactory implements ChannelHandlerFactory
{
    protected final Logger accessLogger = LoggerFactory.getLogger("org.dcache.access.xrootd");

    protected final AccessLogHandler handler = new AccessLogHandler(accessLogger);

    @Override
    public String getName()
    {
        return AccessLogHandlerProvider.NAME;
    }

    @Override
    public String getDescription()
    {
        return "Generates an access log";
    }

    @Override
    public ChannelHandler createHandler()
    {
        return handler;
    }
}
