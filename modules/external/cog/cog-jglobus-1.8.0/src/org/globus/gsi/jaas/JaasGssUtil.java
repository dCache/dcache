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

import java.util.Set;
import java.util.Iterator;

import javax.security.auth.Subject;

import org.globus.gsi.gssapi.GlobusGSSName;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSException;

/**
 * An utility class for handling JAAS Subject with GSSCredential.
 */
public class JaasGssUtil {

    /**
     * Creates a new <code>Subject</code> object from specified 
     * <code>GSSCredential</code>. The GSSCredential is added
     * to the private credential set of the Subject object.
     * Also, if the GSSCredential.getName() is of type <code>
     * org.globus.gsi.gssapi.GlobusGSSName</code>
     * a <code>org.globus.gsi.jaas.GlobusPrincipal</code>
     * is added to the principals set of the Subject object.
     */
    public static Subject createSubject(GSSCredential cred) 
	throws GSSException {
	return createSubject(null, cred);
    }

    /**
     * Creates a new <code>Subject</code> object from specified 
     * <code>GSSCredential</code> and <code>GSSName</code>.
     * If the GSSCredential is specified it is added
     * to the private credential set of the Subject object.
     * Also, if the GSSCredential.getName() is of type <code>
     * org.globus.gsi.gssapi.GlobusGSSName</code> and the
     * GSSName parameter was not specified a 
     * <code>org.globus.gsi.jaas.GlobusPrincipal</code>
     * is added to the principals set of the Subject object.
     * If the GSSName parameter was specified of type
     * <code>org.globus.gsi.gssapi.GlobusGSSName</code> a 
     * <code>org.globus.gsi.jaas.GlobusPrincipal</code>
     * is added to the principals set of the Subject object.
     */
    public static Subject createSubject(GSSName name, GSSCredential cred) 
	throws GSSException {
	if (cred == null && name == null) {
	    return null;
	}
	Subject subject = new Subject();
	if (cred != null) {
	    subject.getPrivateCredentials().add(cred);
	    if (name == null) {
		GlobusPrincipal nm = toGlobusPrincipal(cred.getName());
		subject.getPrincipals().add(nm);
	    }
	}
	if (name != null) {
	    GlobusPrincipal nm = toGlobusPrincipal(name);
	    subject.getPrincipals().add(nm);
	}

	return subject;
    }

    /**
     * Converts the specified GSSName to GlobusPrincipal.
     * The GSSName is converted into the GlobusPrincipal
     * only if the GSSName is of type 
     * <code>org.globus.gsi.gssapi.GlobusGSSName</code> 
     * and the name is not anonymous.
     */
    public static GlobusPrincipal toGlobusPrincipal(GSSName name) {
	return (!name.isAnonymous() &&
		(name instanceof GlobusGSSName)) ?
	    new GlobusPrincipal(name.toString()) :
	    null;
    }

    /**
     * Retrieves the first <code>GSSCredential</code> from the
     * private credential set of the specified <code>Subject</code> 
     * object.
     *
     * @return the <code>GSSCredential</code>. Might be null.
     */
    public static GSSCredential getCredential(Subject subject) {
	if (subject == null) {
	    return null;
	}
	Set gssCreds = subject.getPrivateCredentials(GSSCredential.class);
	if (gssCreds == null) {
	    return null;
	}
	Iterator iter = gssCreds.iterator();
	return (iter.hasNext()) ?
	    (GSSCredential)iter.next() :
	    null;
    }
    
}
