/*
 * dCache - http://www.dcache.org/
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
package org.dcache.services.httpd.handlers;

import dmg.util.HttpException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Handles messages having to do with alias configuration errors (in the batch file).
 *
 * @author arossi
 */
public class BadConfigAlias implements AliasHandler {

    private static final String BAD_CONFIG = "HTTP Server badly configured";
    private String message = BAD_CONFIG;

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response)
          throws HttpException {
        throw new HttpException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, message);
    }

    public void setFailureMessage(String failureMessage) {
        if (failureMessage != null) {
            message = BAD_CONFIG + ": " + failureMessage + ".";
        }
    }
}
