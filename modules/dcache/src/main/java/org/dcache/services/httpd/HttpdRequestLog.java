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
package org.dcache.services.httpd;

import org.dcache.util.NetLoggerBuilder;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HttpdRequestLog extends AbstractLifeCycle
    implements RequestLog
{

    private static final Logger ACCESS_LOGGER = LoggerFactory.getLogger("org.dcache.access.httpd");


    public void log(Request request, Response response)
    {
        if(ACCESS_LOGGER.isInfoEnabled()){
            NetLoggerBuilder log = new NetLoggerBuilder(NetLoggerBuilder.Level.TRACE, "org.dcache.httpd.request").omitNullValues();
            log.add("request: ", request);
            log.add("response: ", response);
            log.toLogger(ACCESS_LOGGER);
        }
    }



}
