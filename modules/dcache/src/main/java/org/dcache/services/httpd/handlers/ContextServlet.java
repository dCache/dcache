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

import dmg.cells.nucleus.DomainContextAware;
import dmg.util.HttpException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import org.dcache.services.httpd.exceptions.OnErrorException;

/**
 * Serves a page of the domain context, e.g. robots.txt; 404 if the domain has no such page.
 */
public class ContextServlet extends HttpServlet implements DomainContextAware {

    private static final long serialVersionUID = 1L;

    private final transient ContextAlias alias;

    public ContextServlet(String specificName) {
        alias = new ContextAlias(specificName);
    }

    @Override
    public void setDomainContext(Map<String, Object> context) {
        alias.setDomainContext(context);
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        try {
            alias.handle(request, response);
        } catch (HttpException e) {
            response.sendError(e.getErrorCode(), e.getMessage());
        } catch (OnErrorException e) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }
}
