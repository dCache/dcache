/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2001 - 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.util;

import org.eclipse.jetty.servlet.ServletHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.web.context.WebApplicationContext;

import javax.servlet.ServletContext;

import static org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE;

/**
 * Provide a minimal spring-web compatibility layer for a Jetty server
 * that was instantiated from within a Spring ApplicationContext.
 */
public class SpringWebServletHandler extends ServletHandler implements ApplicationContextAware
{
    private ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext context)
    {
        this.context = context;
    }

    @Override
    public void doStart() throws Exception
    {
        super.doStart();

        getServletContext().setAttribute(ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE,
                new ForwardingWebApplicationContext());
    }

    /**
     * Bridge jetty-web with an existing Spring context and Jetty instance.
     */
    public class ForwardingWebApplicationContext extends ForwardingApplicationContext
            implements WebApplicationContext
    {
        public ForwardingWebApplicationContext()
        {
            super(SpringWebServletHandler.this.context);
        }

        @Override
        public ServletContext getServletContext()
        {
            return SpringWebServletHandler.this.getServletContext();
        }
    }
}
