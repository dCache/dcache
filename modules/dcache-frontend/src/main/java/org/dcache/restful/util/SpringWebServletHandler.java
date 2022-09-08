/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2001 - 2022 Deutsches Elektronen-Synchrotron
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

import static org.springframework.web.context.WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE;

import java.lang.annotation.Annotation;
import javax.servlet.ServletContext;
import org.eclipse.jetty.servlet.ServletHandler;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.ResolvableType;
import org.springframework.web.context.WebApplicationContext;

/**
 * Provide a minimal spring-web compatibility layer for a Jetty server that was instantiated from
 * within a Spring ApplicationContext.
 */
public class SpringWebServletHandler extends ServletHandler implements ApplicationContextAware {

    private ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext context) {
        this.context = context;
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();

        getServletContext().setAttribute(ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE,
              new ForwardingWebApplicationContext());
    }

    /**
     * Bridge jetty-web with an existing Spring context and Jetty instance.
     */
    public class ForwardingWebApplicationContext extends ForwardingApplicationContext
          implements WebApplicationContext {

        public ForwardingWebApplicationContext() {
            super(SpringWebServletHandler.this.context);
        }

        @Override
        public ServletContext getServletContext() {
            return SpringWebServletHandler.this.getServletContext();
        }

        @Override
        public <T> ObjectProvider<T> getBeanProvider(Class<T> aClass, boolean b) {
            return SpringWebServletHandler.this.context.getBeanProvider(aClass, b);
        }

        @Override
        public <T> ObjectProvider<T> getBeanProvider(ResolvableType resolvableType, boolean b) {
            return SpringWebServletHandler.this.context.getBeanProvider(resolvableType, b);
        }

        @Override
        public <A extends Annotation> A findAnnotationOnBean(String s, Class<A> aClass, boolean b)
              throws NoSuchBeanDefinitionException {
            return SpringWebServletHandler.this.context.findAnnotationOnBean(s, aClass, b);
        }
    }
}
