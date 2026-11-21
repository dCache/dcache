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
import java.io.IOException;
import org.dcache.services.httpd.exceptions.OnErrorException;

/**
 * Answers the GET requests of one httpd alias, as created by {@code set alias}.
 * <p>
 * {@link AliasDispatcherServlet} selects the alias by the first element of the request path and
 * turns exceptions into HTTP responses.
 */
@FunctionalInterface
public interface AliasHandler {

    /**
     * Writes the response to {@code request}.
     *
     * @throws HttpException    to answer with the exception's status code and message
     * @throws OnErrorException to answer with the alias given by the {@code -onError} option
     */
    void handle(HttpServletRequest request, HttpServletResponse response)
          throws IOException, HttpException, OnErrorException;
}
