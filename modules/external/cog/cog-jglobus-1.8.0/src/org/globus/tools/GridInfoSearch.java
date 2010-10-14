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
package org.globus.tools;

import java.util.Hashtable;
import java.util.Enumeration;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.directory.Attributes;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.InitialLdapContext;

import org.globus.mds.gsi.common.GSIMechanism;

/** Performs an LDAP  search.
 *
 * grid-info-search [ options ] <search filter> [attributes]
 * example:
 *	grid-info-search "(objectclass=GlobusServiceJobManager)" contact
 */

// we could add: aliasing, referral support
public class GridInfoSearch {

    //Default values
    private static final String version = 
	org.globus.common.Version.getVersion();
    
    private static final String DEFAULT_CTX = 
	"com.sun.jndi.ldap.LdapCtxFactory";

    private String hostname;
    private int port = 2135;
    private String baseDN = "mds-vo-name=local, o=grid";
    private int timeOutInSeconds = 30;
    private int scope = SearchControls.SUBTREE_SCOPE;
    private int ldapVersion = 3;
    private int sizeLimit = 0;
    private int timeLimit = 0;
    private boolean ldapTrace = false;
    private String saslMech = "GSI";
    private String bindDN;
    private String password;
    private String qop = "auth";
    private boolean verbose = false;

    public GridInfoSearch() {
    }

    private String getHostname() {
	if (hostname == null) {
	    try {
		setHostname(InetAddress.getLocalHost().getHostName());
	    } catch ( UnknownHostException e ) {
		System.err.println( "Error getting hostname: " + e.getMessage() );
		System.exit(1);
	    }
	}
	return hostname;
    }

    private void search(String filter, String [] attributes) {

	Hashtable env = new Hashtable();

	String url = "ldap://" + getHostname() + ":" + port;

	if (verbose) {
	    System.out.println("Connecting to: " + url);
	}

	env.put("java.naming.ldap.version", String.valueOf(ldapVersion));
	env.put(Context.INITIAL_CONTEXT_FACTORY, DEFAULT_CTX);
	env.put(Context.PROVIDER_URL, url);

	if (ldapTrace) {
	    env.put("com.sun.jndi.ldap.trace.ber", System.err);
	}

	if (bindDN != null) {
	    env.put(Context.SECURITY_PRINCIPAL, bindDN);
	}

	if (saslMech != null) {
	    
	    if (saslMech.equalsIgnoreCase("GSI") ||
		saslMech.equalsIgnoreCase(GSIMechanism.NAME)) {
		saslMech = GSIMechanism.NAME;
		env.put("javax.security.sasl.client.pkgs", 
			"org.globus.mds.gsi.jndi");
	    }
	    
	    env.put(Context.SECURITY_AUTHENTICATION, saslMech);

	    env.put("javax.security.sasl.qop", qop);

	} else {
	    // default to simple authentication
	    env.put(Context.SECURITY_AUTHENTICATION, "simple");
	    if (password != null) {
		env.put(Context.SECURITY_CREDENTIALS, password);
	    }
	}

	LdapContext ctx = null;
	
	try {
	    ctx = new InitialLdapContext(env, null);

	    SearchControls constraints = new SearchControls();

	    constraints.setSearchScope(scope);  
	    constraints.setCountLimit(sizeLimit);
	    constraints.setTimeLimit(timeLimit);
	    constraints.setReturningAttributes(attributes);

	    NamingEnumeration results = ctx.search(baseDN, filter, constraints);

	    displayResults(results);

	} catch (Exception e) {
	    System.err.println("Failed to search: " + e.getMessage());
	    if (verbose) {
		e.printStackTrace();
	    }
	} finally {
	    if (ctx != null) {
		try { ctx.close(); } catch (Exception e) {}
	    }
	}
    }

    private void displayResults(NamingEnumeration results) 
	throws NamingException {

	if (results == null) return;
	
	String dn;
	String attribute;
	Attributes attrs;
	Attribute at;
	SearchResult si;

	while (results.hasMoreElements()) {
	    si = (SearchResult)results.next(); 
	    attrs = si.getAttributes();

	    if (si.getName().trim().length() == 0) {
		dn = baseDN;
	    } else {
		dn = si.getName() + ", " + baseDN;
	    }
	    System.out.println("dn: " + dn);

	    for (NamingEnumeration ae = attrs.getAll(); ae.hasMoreElements();) {
		at = (Attribute)ae.next();
		
		attribute = at.getID();

		Enumeration vals = at.getAll();
		while(vals.hasMoreElements()) {
		    System.out.println(attribute + ": " + vals.nextElement());
		}
	    }
	    System.out.println();
	}
    }

    static public String getVersion() {
	return version;
    }

    private void setScope( int scope ) {
	this.scope = scope;
    }

    private void setLdapVersion ( int version ) {
	this.ldapVersion = version;
    }

    private void setSizeLimit ( int limit ) {
	this.sizeLimit = limit;
    }

    private void setTimeLimit ( int limit ) {
	this.timeLimit = limit;
    }

    private void setLdapTrace( boolean trace ) {
	this.ldapTrace = trace;
    }

    private void setHostname( String hostname ) {
	this.hostname = hostname;
    }

    private void setPort( int port ) {
	this.port = port;
    }

    private void setBaseDN( String baseDN ) {
	this.baseDN = baseDN;
    }

    private void setTimeout( int timeOutInSeconds ) {
	this.timeOutInSeconds = timeOutInSeconds;
    }

    private void setSaslMech( String mech ) {
	this.saslMech = mech;
    }

    private void setQOP( String qop ) {
	this.qop = qop;
    }

    private void setBindDN( String bindDN ) {
	this.bindDN = bindDN;
    }

    private void setPassword( String pwd ) {
	this.password = pwd;
    }

    private void setVerbose( boolean verbose ) {
	this.verbose = verbose;
    }

    private boolean isVerbose() {
	return this.verbose;
    }

    private String getSyntaxString() {
	return
	    "\n"
	    + "Syntax : grid-info-search [ options ] "
	    + "<search filter> [attributes]\n\n"
	    + "Use -help to display full usage.";
    }

    private String getHelpString() {
	return
	    "\n"
	    + "grid-info-search [ options ] <search filter> [attributes]\n\n"
	    + "    Searches the MDS server based on the search filter, where some\n"
	    + "    options are:\n"
	    + "       -help\n"
	    + "               Displays this message\n"
	    + "\n"
	    + "       -version\n"
	    + "               Displays the current version number\n"
	    + "\n"
	    + "       -mdshost host (-h)\n"
	    + "               The host name on which the MDS server is running\n"
	    + "               The default is " + getHostname() + ".\n"
	    + "\n"
	    + "       -mdsport port (-p)\n"
	    + "               The port number on which the MDS server is running\n"
	    + "               The default is " + String.valueOf( port ) + "\n"
	    + "\n"
	    + "       -mdsbasedn branch-point (-b)\n"
	    + "               Location in DIT from which to start the search\n"
	    + "               The default is '" + baseDN + "'\n"
	    + "\n"
	    + "       -mdstimeout seconds (-T)\n"
	    + "               The amount of time (in seconds) one should allow to\n"
	    + "               wait on an MDS request. The default is "
	    + String.valueOf( timeOutInSeconds ) + "\n"
	    + "\n" 
	    + "       -anonymous (-x)\n"
	    + "               Use anonymous binding instead of GSSAPI."
	    + "\n\n"
	    + "     grid-info-search also supports some of the flags that are\n"
	    + "     defined in the LDAP v3 standard.\n" 
	    + "     Supported flags:\n\n" 
	    + "      -s scope   one of base, one, or sub (search scope)\n" 
	    + "      -P version protocol version (default: 3)\n" 
	    + "      -l limit   time limit (in seconds) for search\n" 
	    + "      -z limit   size limit (in entries) for search\n" 
	    + "      -Y mech    SASL mechanism\n"
	    + "      -D binddn  bind DN\n" 
	    + "      -v         run in verbose mode (diagnostics to standard output)\n"
	    + "      -O props   SASL security properties (auth, auth-conf, auth-int)\n" 
	    + "      -w passwd  bind password (for simple authentication)\n"
	    + "\n";
    }

    private static String getValue(int i, String [] args, String option) {
	if ( i >= args.length ) {
	    System.err.println("Error: argument required for : " + option);
	    System.exit(1);
	}
	return args[i];
    }

    private static int getValueAsInt(int i, String [] args, String option) {
	String value = getValue(i, args, option);
	try {
	    return Integer.parseInt(value);
	} catch (Exception e) {
	    System.err.println("Error: value '" + value + "' is not an integer for : " + option);
	    System.exit(1);
	    return -1;
	}
    }

    public static void main( String [] args ) {

	GridInfoSearch gridInfoSearch = new GridInfoSearch();

	int i;
	for (i=0;i<args.length;i++) {
	    if (args[i].startsWith("-")) {
		
		String option = args[i];

		// no arg required
		if ( option.equalsIgnoreCase( "-ldapTrace" ) ) {
		    gridInfoSearch.setLdapTrace(true);
		} else if ( option.equalsIgnoreCase( "-help" ) ) {
		    System.err.println( gridInfoSearch.getHelpString() );
		    System.exit( 0 );
		} else if ( option.equalsIgnoreCase( "-version" ) ) {
		    System.err.println( GridInfoSearch.getVersion() );
		    System.exit( 0 );
		} else if ( option.equalsIgnoreCase( "-mdshost" ) ||
			    option.equals( "-h" ) ) {
		    gridInfoSearch.setHostname( getValue(++i, args, option) );
		} else if ( option.equalsIgnoreCase( "-mdsport" ) ||
			    option.equals( "-p" ) ) {
		    gridInfoSearch.setPort( getValueAsInt(++i, args, option) );
		} else if ( option.equalsIgnoreCase( "-mdsbasedn" ) ||
			    option.equals( "-b" ) ) {
		    gridInfoSearch.setBaseDN(  getValue(++i, args, option) );
		} else if ( option.equalsIgnoreCase( "-mdstimeout" ) ||
			    option.equals( "-T" ) ) {
		    gridInfoSearch.setTimeout( getValueAsInt(++i, args, option) );
		} else if ( option.equals( "-s" ) ) {
		    String value = getValue(++i, args, option);
		    if (value.equalsIgnoreCase("one")) {
			gridInfoSearch.setScope(SearchControls.ONELEVEL_SCOPE);
		    } else if (value.equalsIgnoreCase("base")) {
			gridInfoSearch.setScope(SearchControls.OBJECT_SCOPE);
		    } else if (value.equalsIgnoreCase("sub")) {
			gridInfoSearch.setScope(SearchControls.SUBTREE_SCOPE);
		    } else {
			System.err.println("Error: invalid scope parameter : " + value);
			System.exit(1);
		    }
		} else if ( option.equals( "-P" ) ) {
		    gridInfoSearch.setLdapVersion( getValueAsInt(++i, args, option) );
		} else if ( option.equals( "-l" ) ) {
		    gridInfoSearch.setTimeLimit(  getValueAsInt(++i, args, option) );
		} else if ( option.equals( "-z" ) ) {
		    gridInfoSearch.setSizeLimit( getValueAsInt(++i, args, option) );
		} else if ( option.equals( "-Y" ) ) {
		    gridInfoSearch.setSaslMech(  getValue(++i, args, option) );
		} else if ( option.equals( "-D" ) ) {
		    gridInfoSearch.setBindDN(  getValue(++i, args, option) );
		} else if ( option.equals( "-v" ) ) {
		    gridInfoSearch.setVerbose(true);
		} else if ( option.equals( "-x" ) ||
			    option.equalsIgnoreCase( "-anonymous" ) ) {
		    gridInfoSearch.setSaslMech( null );
		} else if ( option.equals( "-O" )) {
		    gridInfoSearch.setQOP( getValue(++i, args, option) );
		} else if ( option.equals( "-w" ) ) {
		    gridInfoSearch.setPassword( getValue(++i, args, option) );
		} else {
		    System.err.println("Error: unrecognized argument : " + option);
		    System.exit(1);
		}
	    } else {
		break;
	    }
	}
	
	String filter = null;
	if (i == args.length) {
	    filter = "(objectclass=*)";
	} else {
	    filter = args[i];
	}
	
	String [] attribs = null;

	if (++i < args.length) {
	    int size = args.length - i;
	    attribs = new String[size];
	    System.arraycopy(args, i, attribs, 0, size);
	}

	if (gridInfoSearch.isVerbose()) {
	    System.out.println("filter: " + filter);
	    if (attribs == null) {
		System.out.println("attribs: none");
	    } else {
		System.out.print("attribs: ");
		for (i=0;i<attribs.length;i++) {
		    System.out.print(attribs[i] + " ");
		}
		System.out.println();
	    }
	}

	gridInfoSearch.search(filter, attribs);
    }
    
}
