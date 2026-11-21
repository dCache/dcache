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

import static java.util.Objects.requireNonNull;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.Servlet;
import java.util.EnumSet;
import java.util.EventListener;
import java.util.List;
import java.util.Map;
import org.dcache.util.spring.Required;
import org.eclipse.jetty.ee11.servlet.FilterHolder;
import org.eclipse.jetty.ee11.servlet.FilterMapping;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.ee11.servlet.SessionHandler;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.eclipse.jetty.util.resource.Resources;
import org.springframework.beans.factory.config.AbstractFactoryBean;

/**
 * Creates the servlet context of a door: an ordered chain of filters in front of servlets, each
 * mapped to servlet path specs.
 * <p>
 * A request passes the filters whose path specs match, in the order given, then the filters mapped
 * to the servlet that serves it, in the order given, and then reaches the servlet with the best
 * matching path spec. A filter that does not call
 * {@link jakarta.servlet.FilterChain#doFilter} handles the request itself; the rest of the chain is
 * skipped. Filters are applied to requests from clients only, not to forwards or includes.
 * <p>
 * The product is the core Jetty {@link Handler} of the context, to be used as the handler of the
 * server or of a core handler wrapper.
 */
public class ServletContextFactoryBean extends AbstractFactoryBean<Handler> {

    private String contextPath = "/";
    private SessionHandler sessionHandler;
    private SecurityHandler securityHandler;
    private List<FilterEntry> filters = List.of();
    private List<ServletEntry> servlets = List.of();
    private List<String> baseResources = List.of();
    private List<EventListener> eventListeners = List.of();

    public void setContextPath(String contextPath) {
        this.contextPath = requireNonNull(contextPath);
    }

    /**
     * Sets the directories of the context's static content, as served by a {@code DefaultServlet}
     * without a base resource of its own: a file is taken from the first directory that has it.
     * Directories that do not exist are skipped.
     */
    public void setBaseResources(List<String> baseResources) {
        this.baseResources = List.copyOf(baseResources);
    }

    /**
     * Sets the session handler; without one, the context has no sessions.
     */
    public void setSessionHandler(SessionHandler sessionHandler) {
        this.sessionHandler = sessionHandler;
    }

    /**
     * Sets the security handler, which runs before the filters.
     */
    public void setSecurityHandler(SecurityHandler securityHandler) {
        this.securityHandler = securityHandler;
    }

    /**
     * Sets the listeners of the context, e.g. a {@code ServletContextListener}, which is notified
     * before the servlets are initialised.
     */
    public void setEventListeners(List<EventListener> eventListeners) {
        this.eventListeners = List.copyOf(eventListeners);
    }

    public void setFilters(List<FilterEntry> filters) {
        this.filters = List.copyOf(filters);
    }

    @Required
    public void setServlets(List<ServletEntry> servlets) {
        this.servlets = List.copyOf(servlets);
    }

    @Override
    public Class<?> getObjectType() {
        return Handler.class;
    }

    @Override
    protected Handler createInstance() {
        ServletContextHandler context = new ServletContextHandler(contextPath,
              sessionHandler, securityHandler, null, null);
        eventListeners.forEach(context::addEventListener);
        if (!baseResources.isEmpty()) {
            ResourceFactory factory = ResourceFactory.of(context);
            List<Resource> resources = baseResources.stream()
                  .map(factory::newResource)
                  .filter(r -> !Resources.missing(r))
                  .toList();
            if (!resources.isEmpty()) {
                context.setBaseResource(ResourceFactory.combine(resources));
            }
        }
        ServletHandler servletHandler = context.getServletHandler();
        /* File names may contain what Servlet 6 calls ambiguous: "%25", "%2F", empty or
         * encoded "." segments. Decode such paths as Jetty 9 did; the connector's URI compliance
         * decides which paths are accepted at all.
         */
        servletHandler.setDecodeAmbiguousURIs(true);
        for (FilterEntry entry : filters) {
            FilterHolder holder = new FilterHolder(entry.filter);
            if (entry.name != null) {
                holder.setName(entry.name);
            }
            holder.setInitParameters(entry.initParameters);
            servletHandler.addFilter(holder);

            FilterMapping mapping = new FilterMapping();
            mapping.setFilterName(holder.getName());
            mapping.setDispatcherTypes(EnumSet.of(DispatcherType.REQUEST));
            if (entry.servletNames.isEmpty()) {
                mapping.setPathSpecs(entry.pathSpecs.toArray(String[]::new));
            } else {
                mapping.setServletNames(entry.servletNames.toArray(String[]::new));
            }
            servletHandler.addFilterMapping(mapping);
        }
        for (ServletEntry entry : servlets) {
            ServletHolder holder = new ServletHolder(entry.servlet);
            if (entry.name != null) {
                holder.setName(entry.name);
            }
            holder.setInitParameters(entry.initParameters);
            for (String pathSpec : entry.pathSpecs) {
                context.addServlet(holder, pathSpec);
            }
        }
        return context;
    }

    /**
     * A filter of the context.
     */
    public static class FilterEntry {

        private Filter filter;
        private String name;
        private List<String> pathSpecs = List.of("/*");
        private List<String> servletNames = List.of();
        private Map<String, String> initParameters = Map.of();

        @Required
        public void setFilter(Filter filter) {
            this.filter = requireNonNull(filter);
        }

        /**
         * Sets the name of the filter, as shown in logs; generated if not set.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Sets the servlet path specs to which the filter applies; all paths by default.
         */
        public void setPathSpecs(List<String> pathSpecs) {
            this.pathSpecs = List.copyOf(pathSpecs);
        }

        /**
         * Sets the names of the servlets to which the filter applies, instead of path specs.
         */
        public void setServletNames(List<String> servletNames) {
            this.servletNames = List.copyOf(servletNames);
        }

        public void setInitParameters(Map<String, String> initParameters) {
            this.initParameters = Map.copyOf(initParameters);
        }
    }

    /**
     * A servlet of the context.
     */
    public static class ServletEntry {

        private Servlet servlet;
        private String name;
        private List<String> pathSpecs;
        private Map<String, String> initParameters = Map.of();

        @Required
        public void setServlet(Servlet servlet) {
            this.servlet = requireNonNull(servlet);
        }

        /**
         * Sets the name of the servlet, as shown in logs; generated if not set.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Sets the servlet path specs the servlet serves, e.g. {@code /static/*} or {@code /}.
         */
        @Required
        public void setPathSpecs(List<String> pathSpecs) {
            this.pathSpecs = List.copyOf(pathSpecs);
        }

        public void setInitParameters(Map<String, String> initParameters) {
            this.initParameters = Map.copyOf(initParameters);
        }
    }
}
