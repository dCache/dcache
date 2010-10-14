/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.gsi.jaas;

import org.globus.util.I18n;

import javax.security.auth.Subject;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;

/**
 * Generic JAAS Subject helper API that provides abstraction layer on top of 
 * vendor-specific JAAS Subject extensions implementations. 
 * Most vendors defined their own JAAS Subject helper classes because of the 
 * <a href="http://publib7b.boulder.ibm.com/wasinfo1/en/info/aes/ae/rsec_jaasauthor.html">
 * Subject propagation issue</a> in JAAS.
 */
public abstract class JaasSubject {

    private static I18n i18n =
            I18n.getI18n("org.globus.gsi.gssapi.errors",
                         JaasSubject.class.getClassLoader());
    private static JaasSubject subject;
    
    protected JaasSubject() {}
    
    /**
     * Gets current implementation of the <code>JaasSubject</code> API.
     * The method attempts to load a <code>JaasSubject</code> implementation
     * by loading a class specified by the 
     * "<i>org.globus.jaas.provider</i>" system property. If the property
     * is not set the default Globus implementation is loaded.
     */
    public static synchronized JaasSubject getJaasSubject() {
	if (subject == null) {
	    String className = System.getProperty("org.globus.jaas.provider");
	    if (className == null) {
		className = "org.globus.gsi.jaas.GlobusSubject";
	    }
	    try {
		Class clazz = Class.forName(className);
		if (!JaasSubject.class.isAssignableFrom(clazz)) {
		    throw new RuntimeException(i18n.getMessage("invalidJaasSubject",
                                                       className));
		}
		subject = (JaasSubject)clazz.newInstance();
	    } catch (ClassNotFoundException e) {
		throw new RuntimeException(i18n.getMessage("loadError", className) +
                                   e.getMessage());
	    } catch (InstantiationException e) {
		throw new RuntimeException(i18n.getMessage("instanError", className) +
                                   e.getMessage());
	    } catch (IllegalAccessException e) {
		throw new RuntimeException(i18n.getMessage("instanError", className)
                                   + e.getMessage());
	    }
	}
	return subject;
    }

    // SPI 
    /**
     * SPI method. 
     */
    public abstract Subject getSubject();
    
    /**
     * SPI method. 
     */
    public abstract Object runAs(Subject subject, PrivilegedAction action);

    /**
     * SPI method. 
     */
    public abstract Object runAs(Subject subject, PrivilegedExceptionAction action)
	throws PrivilegedActionException;
    
    // API
    
    /**
     * A convenience method, calls 
     * <code>JaasSubject.getJaasSubject().runAs()<code/>.
     */
    public static Object doAs(Subject subject, PrivilegedExceptionAction action) 
	throws PrivilegedActionException {
	return JaasSubject.getJaasSubject().runAs(subject, action);
    }
    
    /**
     * A convenience method, calls 
     * <code>JaasSubject.getJaasSubject().runAs()<code/>.
     */
    public static Object doAs(Subject subject, PrivilegedAction action) {
	return JaasSubject.getJaasSubject().runAs(subject, action);
    }
    
    /**
     * A convenience method, calls 
     * <code>JaasSubject.getJaasSubject().getSubject()<code/>.
     */
    public static Subject getCurrentSubject() {
	return JaasSubject.getJaasSubject().getSubject();
    }
    
}
