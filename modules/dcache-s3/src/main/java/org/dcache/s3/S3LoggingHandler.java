/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.s3;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.http.AbstractLoggingHandler;
import org.dcache.util.NetLoggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the access log of the S3 door, adding the access key and S3 operation of each request.
 */
public class S3LoggingHandler extends AbstractLoggingHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger("org.dcache.access.s3");

    @Override
    protected Logger accessLogger() {
        return LOGGER;
    }

    @Override
    protected String requestEventName() {
        return "org.dcache.s3.request";
    }

    @Override
    protected void describeOperation(NetLoggerBuilder log, HttpServletRequest request,
          HttpServletResponse response) {
        super.describeOperation(log, request, response);
        log.add("s3.access-key", request.getAttribute(S3JettyHandler.ACCESS_KEY_ATTRIBUTE));
        log.add("s3.operation", request.getAttribute(S3JettyHandler.OPERATION_ATTRIBUTE));
    }
}
