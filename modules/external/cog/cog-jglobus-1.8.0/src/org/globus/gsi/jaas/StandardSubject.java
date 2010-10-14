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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;

import javax.security.auth.Subject;

/**
 * Standard JAAS implementation of the JAAS Subject helper API.
 * This implementation (because of a problem in JAAS) can cut off
 * the Subject object from the thread context.
 */
public class StandardSubject extends JaasSubject {
    
    protected StandardSubject() {
	super();
    }
    
    public Subject getSubject() {
	return Subject.getSubject(AccessController.getContext());
    }
    
    public Object runAs(Subject subject, PrivilegedAction action) {
	return Subject.doAs(subject, action);
    }
    
    public Object runAs(Subject subject, PrivilegedExceptionAction action)
	throws PrivilegedActionException {
	return Subject.doAs(subject, action);
    }
}
