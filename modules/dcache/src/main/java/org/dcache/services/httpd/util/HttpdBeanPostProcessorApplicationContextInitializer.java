/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.services.httpd.util;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Injects dCache service configuration into Spring ApplicationContext environment.
 */
public class HttpdBeanPostProcessorApplicationContextInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext>
{
    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        try {
            Context initialContext = new InitialContext();
            AutowireCapableBeanFactory beanFactory = (AutowireCapableBeanFactory) initialContext.lookup("java:comp/env/beanFactory");
            applicationContext.addBeanFactoryPostProcessor(configurableListableBeanFactory -> {
                configurableListableBeanFactory.addBeanPostProcessor(new BeanPostProcessor()
                {
                    @Override
                    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException
                    {
                        return beanFactory.applyBeanPostProcessorsBeforeInitialization(bean, beanName);
                    }

                    @Override
                    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException
                    {
                        return beanFactory.applyBeanPostProcessorsAfterInitialization(bean, beanName);
                    }
                });
            });
        } catch (NamingException e) {
            throw new IllegalStateException("Outer bean factory not found in JNDI.", e);
        }
    }
}
