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

import javax.security.auth.Subject;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import java.util.LinkedList;

/**
 * An implementation of the <code>JaasSubject</code> API to circumvent
 * the JAAS problem of Subject propagation. The implementation uses
 * a stackable version of 
 * {@link java.lang.InheritableThreadLocal InheritableThreadLocal} 
 * class to associate the Subject object with the current thread.
 * Any new thread started within a thread that has a Subject object
 * associated with it, will inherit the parent's Subject object.
 * Also, nested <code>doAs</code>, <code>runAs</code> calls are supported.
 */
public class GlobusSubject extends JaasSubject {

    private static StackableInheritableThreadLocal subjects = 
	new StackableInheritableThreadLocal();
    
    protected GlobusSubject() {
	super();
    }

    public Subject getSubject() {
	return (Subject)subjects.peek();
    }
    
    public Object runAs(Subject subject, PrivilegedAction action) {
	subjects.push(subject);
	try {
	    return Subject.doAs(subject, action);
	} finally {
	    subjects.pop();
	}
    }

    public Object runAs(Subject subject, PrivilegedExceptionAction action)
	throws PrivilegedActionException {
	subjects.push(subject);
	try {
	    return Subject.doAs(subject, action);
	} finally {
	    subjects.pop();
	}
    }
    
}

class StackableInheritableThreadLocal extends InheritableThreadLocal {
    
    protected Object initialValue() {
	return new LinkedList();
    }

    protected Object childValue(Object parentValue) {
	LinkedList list = (LinkedList)parentValue;
	LinkedList newList = new LinkedList();
	if (!list.isEmpty()) {
	    newList.add(list.getLast());
	}
	return newList;
    }

    public void push(Object object) {
	LinkedList list = (LinkedList)get();
	list.add(object);
    }
    
    public Object pop() {
	LinkedList list = (LinkedList)get();
	return (list.isEmpty()) ? null : list.removeLast();
    }
    
    public Object peek() {
	LinkedList list = (LinkedList)get();
	return (list.isEmpty()) ? null : list.getLast();
    }

}
