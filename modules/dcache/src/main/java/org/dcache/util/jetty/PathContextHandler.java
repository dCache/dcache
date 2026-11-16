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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee8.nested.HandlerWrapper;
import org.eclipse.jetty.ee8.nested.Request;

/**
 * Routes requests with a given path prefix to the wrapped handler.
 * <p>
 * Replaces Jetty 9 {@code ContextHandler}s nested inside another context, which the Jetty 12 ee8
 * environment does not support. The wrapped handler sees the prefix as part of the context path
 * and the rest of the path as path info, as it would inside a real nested context. Request
 * dispatchers, e.g. used by {@code ResourceHandler} to serve welcome files, resolve paths relative
 * to the prefix. Requests that do not match are left unhandled.
 */
public class PathContextHandler extends HandlerWrapper {

    private String contextPath = "/";
    private boolean allowNullPathInfo;

    public String getContextPath() {
        return contextPath;
    }

    public void setContextPath(String contextPath) {
        this.contextPath = contextPath.length() > 1 && contextPath.endsWith("/")
              ? contextPath.substring(0, contextPath.length() - 1)
              : contextPath;
    }

    public boolean getAllowNullPathInfo() {
        return allowNullPathInfo;
    }

    /**
     * If false (the default), a request for the prefix itself is redirected to the prefix with a
     * trailing slash.
     */
    public void setAllowNullPathInfo(boolean allowNullPathInfo) {
        this.allowNullPathInfo = allowNullPathInfo;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
          HttpServletResponse response) throws IOException, ServletException {
        String pathInfo;
        if (target.equals(contextPath)) {
            if (!allowNullPathInfo) {
                baseRequest.setHandled(true);
                String query = request.getQueryString();
                response.sendRedirect(request.getRequestURI() + "/"
                      + (query == null ? "" : "?" + query));
                return;
            }
            pathInfo = null;
        } else if (target.startsWith(contextPath + "/")) {
            pathInfo = target.substring(contextPath.length());
        } else {
            return;
        }

        HttpServletRequest wrapped = new PathRequest(request,
              request.getContextPath() + contextPath, pathInfo);
        super.handle(pathInfo == null ? "/" : pathInfo, baseRequest, wrapped, response);
    }

    /**
     * The request as seen by the wrapped handler.
     */
    private class PathRequest extends HttpServletRequestWrapper {

        private final String contextPath;
        private final String pathInfo;
        private final ServletContext servletContext;

        PathRequest(HttpServletRequest request, String contextPath, String pathInfo) {
            super(request);
            this.contextPath = contextPath;
            this.pathInfo = pathInfo;
            this.servletContext = prefixDispatchers(request.getServletContext());
        }

        @Override
        public ServletContext getServletContext() {
            return servletContext;
        }

        @Override
        public String getContextPath() {
            return contextPath;
        }

        @Override
        public String getServletPath() {
            return "";
        }

        @Override
        public String getPathInfo() {
            return pathInfo;
        }
    }

    /**
     * Returns a view of the enclosing context where {@code getRequestDispatcher} resolves paths
     * relative to the prefix. Its dispatchers pass on the request without this handler's view of
     * the paths, as the target is handled from the enclosing context again.
     */
    private ServletContext prefixDispatchers(ServletContext context) {
        if (context == null) {
            return null;
        }
        return (ServletContext) Proxy.newProxyInstance(getClass().getClassLoader(),
              new Class<?>[]{ServletContext.class},
              (proxy, method, args) -> {
                  if (method.getName().equals("getRequestDispatcher")
                        && args[0] instanceof String path && path.startsWith("/")) {
                      RequestDispatcher dispatcher = context.getRequestDispatcher(contextPath + path);
                      return dispatcher == null ? null : new RequestDispatcher() {
                          @Override
                          public void forward(ServletRequest request, ServletResponse response)
                                throws ServletException, IOException {
                              dispatcher.forward(unwrap(request), response);
                          }

                          @Override
                          public void include(ServletRequest request, ServletResponse response)
                                throws ServletException, IOException {
                              dispatcher.include(unwrap(request), response);
                          }
                      };
                  }
                  try {
                      return method.invoke(context, args);
                  } catch (InvocationTargetException e) {
                      throw e.getCause();
                  }
              });
    }

    private static ServletRequest unwrap(ServletRequest request) {
        return request instanceof PathRequest pathRequest ? pathRequest.getRequest() : request;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + contextPath + "}";
    }
}
