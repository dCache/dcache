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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.context.NoSuchMessageException;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Locale;
import java.util.Map;

/**
 * An implementation of ApplicationContext that delegates all requests to
 * some other ApplicationContext.
 */
public class ForwardingApplicationContext implements ApplicationContext
{
    private final ApplicationContext inner;

    public ForwardingApplicationContext(ApplicationContext context)
    {
        this.inner = context;
    }

    @Override
    public String getId()
    {
        return inner.getId();
    }

    @Override
    public String getApplicationName()
    {
        return inner.getApplicationName();
    }

    @Override
    public String getDisplayName()
    {
        return inner.getDisplayName();
    }

    @Override
    public long getStartupDate()
    {
        return inner.getStartupDate();
    }

    @Override
    public ApplicationContext getParent()
    {
        return inner.getParent();
    }

    @Override
    public AutowireCapableBeanFactory getAutowireCapableBeanFactory() throws IllegalStateException
    {
        return inner.getAutowireCapableBeanFactory();
    }

    @Override
    public Environment getEnvironment()
    {
        return inner.getEnvironment();
    }

    @Override
    public boolean containsBeanDefinition(String string)
    {
        return inner.containsBeanDefinition(string);
    }

    @Override
    public int getBeanDefinitionCount()
    {
        return inner.getBeanDefinitionCount();
    }

    @Override
    public String[] getBeanDefinitionNames()
    {
        return inner.getBeanDefinitionNames();
    }

    @Override
    public String[] getBeanNamesForType(ResolvableType rt)
    {
        return inner.getBeanNamesForType(rt);
    }

    @Override
    public String[] getBeanNamesForType(ResolvableType resolvableType, boolean b, boolean b1) {
        return inner.getBeanNamesForType(resolvableType, b, b1);
    }

    @Override
    public String[] getBeanNamesForType(Class<?> type)
    {
        return inner.getBeanNamesForType(type);
    }

    @Override
    public String[] getBeanNamesForType(Class<?> type, boolean bln, boolean bln1)
    {
        return inner.getBeanNamesForType(type, bln, bln1);
    }

    @Override
    public <T> Map<String, T> getBeansOfType(Class<T> type) throws BeansException
    {
        return inner.getBeansOfType(type);
    }

    @Override
    public <T> Map<String, T> getBeansOfType(Class<T> type, boolean bln, boolean bln1) throws BeansException
    {
        return inner.getBeansOfType(type, bln, bln1);
    }

    @Override
    public String[] getBeanNamesForAnnotation(Class<? extends Annotation> type)
    {
        return inner.getBeanNamesForAnnotation(type);
    }

    @Override
    public Map<String, Object> getBeansWithAnnotation(Class<? extends Annotation> type) throws BeansException
    {
        return inner.getBeansWithAnnotation(type);
    }

    @Override
    public <A extends Annotation> A findAnnotationOnBean(String string, Class<A> type) throws NoSuchBeanDefinitionException
    {
        return inner.findAnnotationOnBean(string, type);
    }

    @Override
    public Object getBean(String string) throws BeansException
    {
        return inner.getBean(string);
    }

    @Override
    public <T> T getBean(String string, Class<T> type) throws BeansException
    {
        return inner.getBean(string, type);
    }

    @Override
    public <T> T getBean(Class<T> type) throws BeansException
    {
        return inner.getBean(type);
    }

    @Override
    public Object getBean(String string, Object... os) throws BeansException
    {
        return inner.getBean(string, os);
    }

    @Override
    public <T> T getBean(Class<T> type, Object... os) throws BeansException
    {
        return inner.getBean(type, os);
    }

    @Override
    public boolean containsBean(String string)
    {
        return inner.containsBean(string);
    }

    @Override
    public boolean isSingleton(String string) throws NoSuchBeanDefinitionException
    {
        return inner.isSingleton(string);
    }

    @Override
    public boolean isPrototype(String string) throws NoSuchBeanDefinitionException
    {
        return inner.isPrototype(string);
    }

    @Override
    public boolean isTypeMatch(String string, ResolvableType rt) throws NoSuchBeanDefinitionException
    {
        return inner.isTypeMatch(string, rt);
    }

    @Override
    public boolean isTypeMatch(String string, Class<?> type) throws NoSuchBeanDefinitionException
    {
        return inner.isTypeMatch(string, type);
    }

    @Override
    public Class<?> getType(String string) throws NoSuchBeanDefinitionException
    {
        return inner.getType(string);
    }

    @Override
    public Class<?> getType(String s, boolean b) throws NoSuchBeanDefinitionException {
        return inner.getType(s, b);
    }

    @Override
    public String[] getAliases(String string)
    {
        return inner.getAliases(string);
    }

    @Override
    public BeanFactory getParentBeanFactory()
    {
        return inner.getParentBeanFactory();
    }

    @Override
    public boolean containsLocalBean(String string)
    {
        return inner.containsLocalBean(string);
    }

    @Override
    public String getMessage(String string, Object[] os, String string1, Locale locale)
    {
        return inner.getMessage(string, os, string1, locale);
    }

    @Override
    public String getMessage(String string, Object[] os, Locale locale) throws NoSuchMessageException
    {
        return inner.getMessage(string, os, locale);
    }

    @Override
    public String getMessage(MessageSourceResolvable msr, Locale locale) throws NoSuchMessageException
    {
        return inner.getMessage(msr, locale);
    }

    @Override
    public void publishEvent(ApplicationEvent ae)
    {
        inner.publishEvent(ae);
    }

    @Override
    public void publishEvent(Object o)
    {
        inner.publishEvent(o);
    }

    @Override
    public Resource[] getResources(String string) throws IOException
    {
        return inner.getResources(string);
    }

    @Override
    public Resource getResource(String string)
    {
        return inner.getResource(string);
    }

    @Override
    public ClassLoader getClassLoader()
    {
        return inner.getClassLoader();
    }

    @Override
    public <T extends Object> ObjectProvider<T> getBeanProvider(Class<T> type) {
        return inner.getBeanProvider(type);
    }

    @Override
    public <T extends Object> ObjectProvider<T> getBeanProvider(ResolvableType rt) {
        return inner.getBeanProvider(rt);
    }
}
