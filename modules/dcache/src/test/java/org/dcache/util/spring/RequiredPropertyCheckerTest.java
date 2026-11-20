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
package org.dcache.util.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;

public class RequiredPropertyCheckerTest {

    private GenericXmlApplicationContext context;

    @After
    public void tearDown() {
        if (context != null && context.isActive()) {
            context.close();
        }
    }

    @Test
    public void shouldStartWhenRequiredPropertyIsSet() {
        start("<bean id='bean' class='" + Bean.class.getName() + "'>"
              + "  <property name='name' value='foo'/>"
              + "</bean>");

        assertThat(context.getBean(Bean.class).name, is("foo"));
    }

    @Test
    public void shouldFailWhenRequiredPropertyIsMissing() {
        BeanCreationException e = assertThrows(BeanCreationException.class,
              () -> start("<bean id='bean' class='" + Bean.class.getName() + "'/>"));

        assertThat(e.getMostSpecificCause(), instanceOf(BeanInitializationException.class));
        assertThat(e.getMostSpecificCause().getMessage(),
              is("Property 'name' is required for bean 'bean'"));
    }

    @Test
    public void shouldListAllMissingProperties() {
        BeanCreationException e = assertThrows(BeanCreationException.class,
              () -> start("<bean id='bean' class='" + TwoRequired.class.getName() + "'/>"));

        assertThat(e.getMostSpecificCause().getMessage(),
              is("Properties 'first', 'second' are required for bean 'bean'"));
    }

    @Test
    public void shouldAcceptNullValue() {
        start("<bean id='bean' class='" + Bean.class.getName() + "'>"
              + "  <property name='name'><null/></property>"
              + "</bean>");

        assertThat(context.getBean(Bean.class).name, is(nullValue()));
    }

    @Test
    public void shouldIgnoreOptionalProperty() {
        start("<bean id='bean' class='" + Bean.class.getName() + "'>"
              + "  <property name='name' value='foo'/>"
              + "</bean>");

        assertThat(context.getBean(Bean.class).optional, is(nullValue()));
    }

    @Test
    public void shouldCheckInheritedSetter() {
        BeanCreationException e = assertThrows(BeanCreationException.class,
              () -> start("<bean id='bean' class='" + SubBean.class.getName() + "'/>"));

        assertThat(e.getMostSpecificCause().getMessage(), containsString("'name'"));
    }

    @Test
    public void shouldCheckOverriddenSetter() {
        BeanCreationException e = assertThrows(BeanCreationException.class,
              () -> start("<bean id='bean' class='" + OverridingBean.class.getName() + "'/>"));

        assertThat(e.getMostSpecificCause().getMessage(), containsString("'name'"));
    }

    @Test
    public void shouldCheckInnerBean() {
        BeanCreationException e = assertThrows(BeanCreationException.class,
              () -> start("<bean id='holder' class='" + Holder.class.getName() + "'>"
                    + "  <property name='bean'><bean class='" + Bean.class.getName() + "'/>"
                    + "  </property>"
                    + "</bean>"));

        assertThat(e.getMostSpecificCause().getMessage(), containsString("'name'"));
    }

    @Test
    public void shouldSkipBeanCreatedByFactoryBean() {
        start("<bean id='factory' class='" + Factory.class.getName() + "'/>"
              + "<bean id='bean' factory-bean='factory' factory-method='create'/>");

        assertThat(context.getBean("bean", Bean.class).name, is("from-factory"));
    }

    private void start(String beans) {
        String xml = "<?xml version='1.0' encoding='UTF-8'?>"
              + "<beans xmlns='http://www.springframework.org/schema/beans'"
              + "       xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
              + "       xsi:schemaLocation='http://www.springframework.org/schema/beans"
              + "           http://www.springframework.org/schema/beans/spring-beans.xsd'>"
              + beans
              + "</beans>";
        context = new GenericXmlApplicationContext();
        RequiredPropertyChecker.register(context);
        context.load(new ByteArrayResource(xml.getBytes(UTF_8)));
        context.refresh();
    }

    public static class Bean {

        String name;
        String optional;

        @Required
        public void setName(String name) {
            this.name = name;
        }

        public void setOptional(String optional) {
            this.optional = optional;
        }
    }

    public static class SubBean extends Bean {

    }

    public static class OverridingBean extends Bean {

        @Override
        public void setName(String name) {
            super.setName(name);
        }
    }

    public static class TwoRequired {

        @Required
        public void setFirst(String value) {
        }

        @Required
        public void setSecond(String value) {
        }
    }

    public static class Holder {

        public void setBean(Bean bean) {
        }
    }

    public static class Factory {

        public Bean create() {
            Bean bean = new Bean();
            bean.name = "from-factory";
            return bean;
        }
    }
}
