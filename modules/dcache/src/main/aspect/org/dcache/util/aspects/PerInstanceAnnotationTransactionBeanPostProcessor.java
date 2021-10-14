/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.util.aspects;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * BeanPostProcessor to configure PerInstanceAnnotationTransactionAspect instances.
 */
public class PerInstanceAnnotationTransactionBeanPostProcessor implements BeanPostProcessor,
      BeanFactoryAware {

    private PlatformTransactionManager txManager;
    private BeanFactory beanFactory;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName)
          throws BeansException {
        if (bean instanceof PerInstanceAnnotationTransactionAspect.HasTransactional) {
            PerInstanceAnnotationTransactionAspect aspect = PerInstanceAnnotationTransactionAspect.aspectOf(
                  bean);
            aspect.setTransactionManager(txManager);
            aspect.setBeanFactory(beanFactory);
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName)
          throws BeansException {
        return bean;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    public void setTransactionManager(PlatformTransactionManager txManager) {
        this.txManager = txManager;
    }
}
