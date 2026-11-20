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
import java.io.IOException;
import java.util.Objects;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes redirection from one URL to another.
 *
 * @author arossi
 */
public class RedirectAlias implements AliasHandler {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(RedirectAlias.class);

    private final String fromContext;
    private final String toContext;

    public RedirectAlias(String fromContext, String toContext) {
        if ("<home>".equals(fromContext)) {
            fromContext = "/";
        }
        this.fromContext = fromContext;
        this.toContext = toContext;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response)
          throws IOException, HttpException {
        String target = request.getServletPath() + Objects.toString(request.getPathInfo(), "");
        LOGGER.debug("target: {}", target);

        if (!target.contains(fromContext)) {
            throw new HttpException(HttpServletResponse.SC_NOT_FOUND, "Not found : " + target);
        }
        StringBuilder targetUrl = new StringBuilder(target);
        int i = targetUrl.indexOf(fromContext);
        String newUrl = targetUrl.replace(i, i + fromContext.length(), toContext).toString();
        LOGGER.debug("redirected to: {}", newUrl);
        response.sendRedirect(newUrl);
    }
}
