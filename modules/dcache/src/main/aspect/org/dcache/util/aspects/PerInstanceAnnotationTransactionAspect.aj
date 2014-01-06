/* This class incorporates code from
 *
 *     org.springframework.transaction.aspectj.AnnotationTransactionAspect
 *
 * which is subject to the following license:
 *
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dcache.util.aspects;

import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.aspectj.AbstractTransactionAspect;

/**
 * Advice @Transactional classes and methods with transaction manager
 * controlled transactions.
 *
 * Similar to AnnotationTransactionAspect, but in contrast to AnnotationTransactionAspect
 * this class is not a singleton. Thus it can be used in the presence of multiple
 * Spring ApplicationContext instances.
 *
 * @see org.dcache.util.aspects.PerInstanceAnnotationTransactionBeanPostProcessor
 */
public aspect PerInstanceAnnotationTransactionAspect extends AbstractTransactionAspect perthis(instantiationOfTransactionalClass())
{
    public PerInstanceAnnotationTransactionAspect() {
        super(new AnnotationTransactionAttributeSource(false));
    }

    /**
     * Matches the execution of any public method in a type with the Transactional
     * annotation, or any subtype of a type with the Transactional annotation.
     */
    private pointcut executionOfAnyPublicMethodInAtTransactionalType() :
            execution(public * ((@Transactional *)+).*(..)) && within(@Transactional *);

    /**
     * Matches the execution of any method with the Transactional annotation.
     */
    private pointcut executionOfTransactionalMethod() :
            execution(@Transactional * *(..));

    /**
     * Definition of pointcut from super aspect - matched join points
     * will have Spring transaction management applied.
     */
    protected pointcut transactionalMethodExecution(Object txObject) :
            (executionOfAnyPublicMethodInAtTransactionalType()
                    || executionOfTransactionalMethod() )
                    && this(txObject);

    /**
     * Marker interface to tag classes that have methods subject to transaction demarcation.
     *
     * The marker is needed so we can bind the perthis Aspect instantiation to the constructor
     * invocation. This in turn is needed to let PerInstanceAnnotationTransactionBeanPostProcessor
     * inject the transaction manager during the Spring configuration phase.
     */
    public interface HasTransactional {} // marker

    /**
     * Matches any constructor of classes implementing the HasTransactional marker.
     */
    pointcut instantiationOfTransactionalClass() :
            execution(HasTransactional+.new(..));

    /**
     * Make any class that has transactional methods implement HasTransactional.
     */
    declare parents : hasmethod(@Transactional * *(..)) implements HasTransactional;
    declare parents : @Transactional * implements HasTransactional;
}