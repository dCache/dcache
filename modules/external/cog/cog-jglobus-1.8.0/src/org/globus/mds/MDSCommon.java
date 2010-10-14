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
package org.globus.mds;   

import java.util.*;     
  
import javax.naming.*;
import javax.naming.directory.*;
import javax.naming.ldap.*;

import org.globus.common.*;

/** Commonly used conveniance functions for accessing specific MDS information.
 * 
 */
public class MDSCommon {

    /** what is this used for?
     * 
     * @param hostname specifies the hostname
     * @param schedulertypes 
     */
    private static String constructFilter(String hostname, Vector schedulertypes) {
	StringBuffer filter = new StringBuffer();
	
	filter.append("(&(objectclass=GlobusServiceJobManager)(hn=");
	filter.append(hostname);
	if (hostname.indexOf("*") == -1) filter.append("*");
	filter.append(")");
	
	int size = schedulertypes.size();
	
	if (size != 0) {
	    
	    filter.append("(|");
	    
	    for (int i=0;i<size;i++) {
		filter.append("(schedulertype=");
		filter.append(schedulertypes.elementAt(i));
		filter.append(")");
	    }
	    
	    filter.append(")");
	}
	
	filter.append(")");

	return filter.toString();
    }

    /** returns a vector of Globus gatekeeper contacts based on the hostname
     * 
     * @exception MDSException 
     * @param mds the MDS in which to look for results
     * @param basedn the base DN from which the tree search is started
     * @param hostname specifies the hostname
     * @param schedulertypes 
     */
    public static Vector hostname2contacts(MDS mds, 
					   String basedn,
					   String hostname, 
					   Vector schedulertypes) 
	throws MDSException {

	String filter             = constructFilter(hostname, schedulertypes);
	SearchResult si           = null;
	Attributes attrs          = null;
	Attribute  at             = null;
	Vector contacts           = new Vector();
	NamingEnumeration results = null;

	try {

	    results = mds.searchl(basedn,
				  filter,
				  new String [] {"contact"},
				  MDS.SUBTREE_SCOPE);

	    while (results != null && results.hasMoreElements()) {
		si    = (SearchResult)results.next();
		attrs = si.getAttributes();
		at    = attrs.get("contact");
		
		contacts.addElement(at.get());
	    }
	} catch(NamingException e) {
	    throw new MDSException("Failed to search for contacts", e.getMessage());
	}

	return contacts;
    }

    /** returns a Hashtable of Globus gatekeeper contacts based on the hostname
     * 
     * @exception MDSException 
     * @param mds the MDS in which to look for results
     * @param basedn the base DN from which the tree search is started
     * @param hostname specifies the hostname
     * @param schedulertypes 
     * @return Hashtable of contacts
     */
    public static Hashtable listContacts(MDS mds, 
					 String basedn,
					 String hostname, 
					 Vector schedulertypes) 
	throws MDSException {
	
	String filter             = constructFilter(hostname, schedulertypes);
	SearchResult si           = null;
	Attributes attrs          = null;
	Attribute  at             = null;
	Hashtable contacts        = new Hashtable();
	NamingEnumeration results = null;
	String key                = null;

	try {

	    results = mds.searchl(basedn,
				  filter,
				  new String [] {"contact", "hn", "schedulertype"},
				  MDS.SUBTREE_SCOPE);
	    
	    while (results != null && results.hasMoreElements()) {
		si    = (SearchResult)results.next();
		attrs = si.getAttributes();

		key = attrs.get("hn").get() + "-" + attrs.get("schedulertype").get();
		contacts.put( key, attrs.get("contact").get() );
	    }
	} catch(NamingException e) {
	    throw new MDSException("Failed to search for contacts", e.getMessage());
	}
	
	return contacts;
    }
    
    /** returns what is this used for?
     * 
     * @exception MDSException 
     * @param mds the MDS in which to look for results
     * @param basedn the base DN from which the tree search is started
     * @param userid ?
     * @return a vector of jobs
     */
    public static Vector userJobs(MDS mds, String basedn, String userid) 
	throws MDSException {

	StringBuffer filter       = new StringBuffer();
	
	filter.append("(&(objectclass=GlobusQueueEntry)(globaluserid=");
	filter.append(userid);
	filter.append("))");

	SearchResult si           = null;
	Attributes attrs          = null;
	Attribute  at             = null;
	Vector joblist            = new Vector();
	NamingEnumeration results = null;

	try {

	    results = mds.searchl(basedn,
				  filter.toString(),
				  new String [] {"globaljobid"},
				  MDS.SUBTREE_SCOPE);

	    while (results != null && results.hasMoreElements()) {
		si    = (SearchResult)results.next();
		attrs = si.getAttributes();
		at    = attrs.get("globaljobid");
		
		joblist.addElement(at.get());
	    }
	} catch(NamingException e) {
	    throw new MDSException("Failed to search for user jobs", e.getMessage());
	}

	return joblist;
    }


    /** what is this used for?
     * 
     * @param dn 
     * @return i do not know
     */
    public static String toGlobusDN(String dn) {
	if (dn == null) return null;

	StringTokenizer tokens = new StringTokenizer(dn, ",");
	StringBuffer buf = new StringBuffer();
	while( tokens.hasMoreTokens() ) {
	    buf.insert(0, tokens.nextToken().trim());
	    if (tokens.hasMoreTokens())
		buf.insert(0, "/");
	}
	buf.insert(0, '/');
	
	return buf.toString();
    }

}





