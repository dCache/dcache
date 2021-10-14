/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.http.AbstractLoggingHandler;
import org.dcache.restful.interceptors.LoggingInterceptor;
import org.dcache.util.NetLoggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Frontend door specific logging.
 */
public class LoggingHandler extends AbstractLoggingHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger("org.dcache.access.frontend");

    @Override
    protected Logger accessLogger() {
        return LOGGER;
    }

    @Override
    protected String requestEventName() {
        return "org.dcache.frontend.request";
    }

    @Override
    protected void describeOperation(NetLoggerBuilder log,
          HttpServletRequest request, HttpServletResponse response) {
        super.describeOperation(log, request, response);

        log.add("request.entity", LoggingInterceptor.getRequestEntity(request));
        log.add("response.entity", LoggingInterceptor.getResponseEntity(request));
    }
}
