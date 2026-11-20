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

import static java.util.Objects.requireNonNull;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.PropertyValues;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;

/**
 * Fails bean creation if a setter annotated with {@link Required} is not configured in the bean
 * definition.
 * <p>
 * Port of Spring 5's {@code RequiredAnnotationBeanPostProcessor}, which was removed in Spring 6,
 * with the same semantics: the property must appear in the bean definition's property values
 * (explicitly or by autowiring); a {@code null} value is accepted. Beans created by an instance
 * factory ({@code factory-bean}) are not checked, because the factory may have populated them.
 * <p>
 * Unlike Spring 5's processor, the annotation is also found on the overridden setter of a
 * superclass and on an interface method, so a subclass overriding a {@link Required} setter
 * without repeating the annotation is still checked.
 */
public class RequiredPropertyChecker implements InstantiationAwareBeanPostProcessor {

    private final ConfigurableListableBeanFactory beanFactory;

    /**
     * Names of beans already checked; a prototype is checked only once.
     */
    private final Set<String> checkedBeans = ConcurrentHashMap.newKeySet();

    public RequiredPropertyChecker(ConfigurableListableBeanFactory beanFactory) {
        this.beanFactory = requireNonNull(beanFactory);
    }

    /**
     * Registers a checker with the bean factory of {@code context}. Must be called before the
     * context is refreshed.
     */
    public static void register(ConfigurableApplicationContext context) {
        context.addBeanFactoryPostProcessor(
              factory -> factory.addBeanPostProcessor(new RequiredPropertyChecker(factory)));
    }

    @Override
    public PropertyValues postProcessProperties(PropertyValues pvs, Object bean, String beanName) {
        if (checkedBeans.add(beanName) && !isCreatedByFactoryBean(beanName)) {
            List<String> missing = new ArrayList<>();
            for (PropertyDescriptor pd : BeanUtils.getPropertyDescriptors(bean.getClass())) {
                Method setter = pd.getWriteMethod();
                if (setter != null
                      && AnnotationUtils.findAnnotation(setter, Required.class) != null
                      && !pvs.contains(pd.getName())) {
                    missing.add(pd.getName());
                }
            }
            if (!missing.isEmpty()) {
                checkedBeans.remove(beanName);
                throw new BeanInitializationException(
                      (missing.size() == 1 ? "Property " : "Properties ")
                            + String.join(", ", missing.stream().map(n -> "'" + n + "'").toList())
                            + (missing.size() == 1 ? " is" : " are")
                            + " required for bean '" + beanName + "'");
            }
        }
        return pvs;
    }

    private boolean isCreatedByFactoryBean(String beanName) {
        return beanFactory.containsBeanDefinition(beanName)
              && beanFactory.getBeanDefinition(beanName).getFactoryBeanName() != null;
    }
}
