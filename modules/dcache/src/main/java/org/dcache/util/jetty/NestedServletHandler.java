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
package org.dcache.util.jetty;

import java.io.IOException;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.util.spring.Required;
import org.eclipse.jetty.ee8.nested.AbstractHandler;
import org.eclipse.jetty.ee8.nested.Request;

/**
 * Runs a servlet as a nested Jetty handler; it handles every request.
 * <p>
 * The servlet is not initialised; servlets used here do not use their {@code ServletConfig}. Used
 * by doors whose handler chains are not servlet contexts yet; to be removed with them.
 */
public class NestedServletHandler extends AbstractHandler {

    private Servlet servlet;

    @Required
    public void setServlet(Servlet servlet) {
        this.servlet = servlet;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
          HttpServletResponse response) throws IOException, ServletException {
        if (!isStarted() || baseRequest.isHandled()) {
            return;
        }
        baseRequest.setHandled(true);
        servlet.service(request, response);
    }
}
