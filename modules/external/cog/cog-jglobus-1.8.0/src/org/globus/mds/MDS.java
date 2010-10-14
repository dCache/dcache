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
import java.net.*;  
import javax.naming.*;
import javax.naming.directory.*;
import javax.naming.ldap.*;
import java.beans.*;

import org.globus.common.*;
import org.globus.util.*;

/** The interface to the Monitoring and Discovery Service.
 * <p><b> USING THE MDS CLASS </b>
 * <blockquote>
 * 
 * The intention of the MDS class is to simplify the connection to the
 * Monitoring and Discovery Service (MDS). This includes:
 * <ol>
 * <li> establishing a conection to an MDS Server,
 * <li> quering for contents in the MDS,
 * <li> printing the result of a query, and
 * <li> disconecting from the MDS Server.
 * </ol>
 * The main motivation for this class is to have an intermediate
 * Application Layer which can be easily adapted to different LDAP
 * client libraries (JNDI, Netscape SDK, Microsoft SDK).  To
 * adapt to the differnt API's only a limited number of routines have
 * to be ported.
 * The current release is based on JNDI.
 * </blockquote>
 * <p><b> Connecting</b>
 * <blockquote>
 * The first step in using the class is to establish a connection,
 * which is done in the following way:
 * 
 * <blockquote><i>
 * MDS mds = new MDS("www.globus.org", "389", "o=Globus, c=US");
 * </i></blockquote>
 * 
 * The parameters to the mds class are simply the DNS name of the MDS
 * server  and the port number for the connection.
 * The default values for the connection to the MDS are set to
 * <i>ldap://mds.globus.org:389</i>, which are used in case no parameters
 * are specified:
 * <blockquote><i>
 * MDS mds = new MDS();
 * </i></blockquote>
 * 
 * </blockquote>
 * <p><b> Searching </b>
 * <blockquote>
 * 
 * Searching the MDS is done with LDAP queries in respect to a base
 * directory specified by a distingished name (DN). The top level of
 * a globus related seraches is:
 * <blockquote><i>
 * o=Globus, c=US
 * </i></blockquote>
 * A search is invoked in the following way:
 * 
 * <blockquote><i>
 * NamingEnumeration result; <br>
 * result = mds.search("o=Globus, c=US", "(objectclass=*)", MDS.ONELEVEL_SCOPE);
 * </i></blockquote>
 * The search result is stored in a NamingEnumeration as provided by JNDI.
 * </blockquote>
 * <p><b> Printing </b>
 * <blockquote>
 * A result from a search can be modified by the appropiate JNDI functions.
 * A simple print function is provided which is intended to simplify
 * debugging and programming development:
 * <blockquote><i>
 * mds.print(result, false);
 * </i></blockquote>
 * </blockquote>
 * <p><b> Example</b>
 * <blockquote>
 * The following example shows a program which connets to the MDS, and prints
 * all ComputeResources located at Argonne National Laboratory:
 * 
 * <blockquote><i>
 * MDS mds = new MDS("mds.globus.org", "389");<br>
 * try {<br>
 * mds.connect();<br>
 * String bindDN = <br>
 * "o=Argonne National Laboratory, o=Globus,c=US";<br>
 * Vector result;<br>
 * result = mds.search(bindDN, <br>
 * "(objectclass=GlobusComputeResource)", MDS.ONELEVEL_SCOPE);<br>
 * System.out.println(results);<br>
 * mds.disconnect();<br>
 * } catch(MDSException e) {<br>
 * System.err.println( "Error:"+ e.getLdapMessage() );<br>
 * }<br>
 * </i></blockquote>
 * </blockquote>
 *
 * @deprecated Please use JNDI (with LDAP provider) or Netscape Directory 
 * SDK directly to access MDS.
 */
public class MDS implements java.io.Serializable {

    // ############################################################
    // BEAN PROPERTIES
    // ############################################################
    
    /** The default hostname (mds11.globus.org) */
    private String hostname        = "mds11.globus.org";
    
    /** The default port (391) */
    private String port            = "391";
    
    /** The default baseDN (o=globus, c=us) */
    private String baseDN          = "o=globus, c=us";

    // ############################################################
    // PROTECTED VARIABLES  
    // ############################################################
    
    /**    * 
     */
    protected int limit              = 0;
    /**    * 
     */
    protected int timeout            = 0;
    /**    * 
     */
    protected int version            = 3;
    
  // ############################################################
  // some JNDI specific variables
  // ############################################################

  /**    * 
   */
  public static final int SUBTREE_SCOPE   = SearchControls.SUBTREE_SCOPE;
  /**    */
  public static final int ONELEVEL_SCOPE  = SearchControls.ONELEVEL_SCOPE;
  /**    */
  public static final int OBJECT_SCOPE    = SearchControls.OBJECT_SCOPE;
  
  // The LDAP factory
  /**    */
  protected static final String DEFAULT_CTX = 
  "com.sun.jndi.ldap.LdapCtxFactory";

  // The directory Context
  /**    */
  protected LdapContext ctx = null;

  // The Properties
  /**    */
  protected Properties   env = null;   

  // ############################################################
  // BEAN PROPERTY CHANGE SUPPORT
  // ############################################################
  // **** propertyChangeSupport ****
  /**    */
  private PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport( this );

  /**    */
  public void addPropertyChangeListener( PropertyChangeListener l ) {
    propertyChangeSupport.addPropertyChangeListener( l );
  }

  /**    */
  public void removePropertyChangeListener( PropertyChangeListener l ) {
    propertyChangeSupport.removePropertyChangeListener( l );
  }
  // **** end listenrs impl  ***

  // **** action method ****
/*
  public void propertyChange( final java.beans.PropertyChangeEvent pce ) {
    if( name.equals( "hostname" ) ) {
    } else if( name.equals( "port" ) ) {
    } else if( name.equals( "baseDN" ) ) {
    }
  }
*/
  // **** end action method ****


  
  // ############################################################
  // CONSTRUCTORS
  // ############################################################
  
  /** initilaizes a connetion to the MDS database with its default values
   */
  public MDS () {
    env = new Properties();
  }
  

  /** initilaizes a connetion to the MDS database to the speified
   * host and port
   * 
   * @param hostname specifies the hostname to which to connect
   * @param port is the port on which the conection is established
   */
  public MDS(String hostname, String port) {
    env = new Properties();
    setHostname(hostname);
    setPort(port);
  }

  /** initilaizes a connetion to the MDS database to the speified
   * host and port
   * 
   * @param hostname specifies the hostname to which to connect
   * @param port is the port on which the conection is established
   * @param baseDN is the base DN from which searches are rooted
   */
  public MDS(String hostname, String port, String baseDN ) {
    env = new Properties();
    setHostname(hostname);
    setPort(port);
    setBaseDN(baseDN);
  }


    /** initilaizes a connetion to the MDS database using the specified
     * LDAP URL
     * 
     * @param spec the LDAP URL spec from which connection information is 
     *     derived
     */
    public MDS( GlobusURL spec ) {
	env = new Properties();
	setHostname( spec.getHost() );
	setPort( String.valueOf( spec.getPort() ) );
	try {
	    setBaseDN( URLDecoder.decode(spec.getFile()) );
	} catch(Exception e) {
	    setBaseDN(spec.getFile());
	}
    }
    
  // ############################################################
  // INITILIZATION OF BEAN PROPERTY LOCATION METHODS
  // ############################################################

  /** sets the baseDN to the specified value
   * 
   * @param baseDN 
   */
  public void setBaseDN (String baseDN) {
    String oldBaseDN = this.baseDN;
    this.baseDN = baseDN;
    propertyChangeSupport.firePropertyChange( "baseDN", oldBaseDN, baseDN );
  }

  /** returns the baseDN currently used
   * 
   * @return DN the baseDN
   */
  public String getBaseDN () {
    return baseDN;
  }

  /** Sets the port to the specified value
   * 
   * @param port 
   */
  public void setPort (String port) {
    String oldPort = this.port;
    this.port = port;
    propertyChangeSupport.firePropertyChange( "port", oldPort, port );
  }

  /** gets the port
   * 
   * @return port the port
   */
  public String getPort () {
    return port;
  }

  /** sets the hostname to the specified value
   * 
   * @param hostname 
   */
  public void setHostname (String hostname) {
    String oldHostname = this.hostname;
    this.hostname = hostname;
    propertyChangeSupport.firePropertyChange( "hostname", oldHostname, hostname );
  }

  /** gets the hostname
   * 
   * @return hostname gets the hostname
   */
  public String getHostname () {
    return hostname;
  }

  /** gets the URL
   * 
   * @return the url string
   */
  public String getURL() {
    return "ldap://" + hostname + ":" + port;
  }

  /** sets search timeout
   * 
   * @param newTimeout 
   */
  public void setSearchTimeout(int newTimeout) {
    timeout = newTimeout;
  }

  /** returns search timeout
   * 
   * @return int timeout
   */
  public int getSearchTimeout() {
    return timeout;
  }

  /** sets search limit - number of returned entries
   * 
   * @param newLimit 
   */
  public void setSearchLimit(int newLimit) {
    limit = newLimit;
  }

  /** 
   * gets search limit
   * 
   * @return int search limit
   */
  public int getSearchLimit() {
    return limit;
  }

  /** 
   * sets ldap protocol version
   * 
   * @param newVersion 
   */
  public void setLdapVersion(int newVersion) {
    version = newVersion;
  }

  /** 
   * gets ldap protocol version
   * 
   * @return ldap version 2 or 3
   */
  public int getLdapVersion() {    
    return version;
  }

  // ############################################################
  // CONNECTIONS TO THE MDS
  // ############################################################

  /** connects to the specified server
   * 
   * @throws an MDSexception if unable to connect
   * @exception MDSException 
   */
  public synchronized void connect() throws MDSException {  
    connect(null, null);
  }

  /** connects and authenticates to the specified server
   * 
   * @throws an MDSexception if unable to connect
   * @exception MDSException 
   * @param managerDN specifies the distingushed name of the directory manager
   *      or user
   * @param password specifies the password of the directory manager or user
   */
  public synchronized void connect(String managerDN, String password) 
      throws MDSException {    

      if (managerDN != null && password != null) {
	  env.put(Context.SECURITY_AUTHENTICATION, "simple");
	  env.put(Context.SECURITY_PRINCIPAL, managerDN);
	  env.put(Context.SECURITY_CREDENTIALS, password);
      } 

      env.put("java.naming.ldap.version", String.valueOf( getLdapVersion() ) );
      env.put(Context.INITIAL_CONTEXT_FACTORY, DEFAULT_CTX);
      env.put(Context.PROVIDER_URL, getURL() );

      try {
	  ctx = new InitialLdapContext(env, null);
      } catch (NamingException e) {
	  env.clear();
	  throw new MDSException("Failed to connect.", e);
      }
      
  }
  
  /** disconnects from the MDS server
   * 
   * @throws an MDSexception if failed to disconnect
   */
  public synchronized void disconnect() throws MDSException {
      if (ctx != null) {
	  try {
	      ctx.close();
	  } catch (NamingException e) {
	      throw new MDSException("Failed to disconnect.", e);
	  } 
      }
      ctx = null;
      env.clear();
  }

  /** reconnects to the server.
   * 
   * @throws an MDSexception if unable to reconnect
   */
  public synchronized void reconnect() throws MDSException {
    disconnect();
    connect();
  }

  
  // ############################################################
  // SEARCH THE CONTENTS OF THE MDS
  // ############################################################

  /** issues a specified search to the MDS. The
   * search returns all attributes.
   * 
   * @throws an MDSexception if something goes bad
   * @param filter a common LDAP filter
   * @param searchScope sets the scope of the search
   * @return the result is returned in a hashtable of MDSResult
   */
  public Hashtable search( String filter, int searchScope )
  			throws MDSException {
    return search( baseDN, filter, null, searchScope );
  }

  /** issues a specified search to the MDS starting from the base DN.
   * 
   * @throws an MDSexception if something goes bad
   * @param filter a common LDAP filter
   * @param attrToReturn 
   * @param searchScope sets the scope of the search
   * @return the result is returned in a hashtable of MDSResult
   */
  public Hashtable search( String filter,
			   String [] attrToReturn,
			   int searchScope) 
    		throws MDSException {
    return search( baseDN, filter, attrToReturn, searchScope );
  }

  /** issues a specified search to the MDS starting from the base DN. The
   * search returns all attributes.
   * 
   * @throws an MDSexception if something goes bad
   * @param baseDN 
   * @param filter a common LDAP filter
   * @param searchScope sets the scope of the search
   * @return the result is returned in a hashtable of MDSResult
   */
  public Hashtable search(String baseDN, 
			  String filter, 
			  int searchScope) 
      throws MDSException {
      return search(baseDN, filter, null, searchScope);			
  }

    /** issues a specified search to the MDS starting from the base DN.
     * 
     * @throws an MDSexception if something goes bad
     * @exception MDSException 
     * @param baseDN the base DN from which the 
     * @param filter a common LDAP filter
     * @param attrToReturn an array of attributes to be returned
     * @param searchScope sets the scope of the search
     * @return the result is returned in a hashtable of MDSResult
     */
    public Hashtable search(String baseDN, 
			    String filter,
			    String [] attrToReturn,
			    int searchScope) 
	throws MDSException {
	
	if ( ctx == null ) {
	    throw new MDSException( "Error: search failed -- not connected to MDS", 
				    (String)null );
	}

	try {
	    SearchControls controls = createConstraints(searchScope, attrToReturn);
	    NamingEnumeration results = ctx.search(baseDN, filter, controls);
	    return convertSearchResultsH(results, baseDN);
	} catch(NamingException e) {
	    throw new MDSException("Failed to search.", e);
	} 
    }
  

    /** Issues a search starting from the basDN.
     * 
     * @exception NamingException when an LDAP error occurs.
     * @param baseDN the base dn from which the search is started
     * @param filter the filter for the search
     * @param attrToReturn an array of attributes to return
     * @param searchScope The search scope 
     * @return returns the result as NamingEnumeration
     */
    public NamingEnumeration searchl(String baseDN, 
				     String filter,
				     String [] attrToReturn,
				     int searchScope) 
	throws NamingException {
	
	SearchControls controls = createConstraints(searchScope, attrToReturn);
	NamingEnumeration results = ctx.search(baseDN, filter, controls);
	
	return results;
    }
    
  /** Lists the objects bound to the argument.
   * 
   * @throws an MDSexception if something goes bad
   * @param baseDN 
   * @return the result is returned in a hashtable of MDSResult
   */
  public Hashtable list(String baseDN) throws MDSException {
    try {

      SearchControls constraints = new SearchControls();
      constraints.setSearchScope(ONELEVEL_SCOPE);  
      constraints.setReturningAttributes(new String[] {"objectclass"});

      NamingEnumeration results = ctx.search(baseDN, "(objectclass=*)", constraints);
      return convertSearchResultsH(results, baseDN);
    } catch(NamingException e) {
      throw new MDSException("Failed to list.", e);
    } 
  }

  /** 
   * This methods returns that attributes of the specified distinguished name.
   * 
   * @throws an MDSexception if something goes bad
   * @param dn 
   * @return Attributes the set of values that constitute the distinguished  
   *     name
   */
  public MDSResult getAttributes(String dn) throws MDSException {

    try {
      BasicAttributes attrs = (BasicAttributes)ctx.getAttributes(dn);
      return convertAttributes(attrs);
    } catch(NamingException e){
      throw new MDSException("Failed to retreive attributes.", e);
    }    
    
  }

  /** 
   * This methods returns that attributes of the specified distinguished name.
   * 
   * @throws an MDSexception if something goes bad
   * @param dn 
   * @param attributes 
   * @return Attributes the set a values that constitute the distinguished  
   *     name
   */
  public MDSResult getAttributes(String dn, String [] attributes) throws MDSException {

    try {
      BasicAttributes attrs = (BasicAttributes)ctx.getAttributes(dn, attributes);
      return convertAttributes(attrs);
    } catch(NamingException e){
      throw new MDSException("Failed to retreive attributes.", e);
    }  

  }  


  // ******************* PRIVATE METHODS *****************************************
  
  /**    */
  protected SearchControls createConstraints(int scope, String [] attrToReturn) {
    SearchControls constraints = new SearchControls();

    constraints.setSearchScope(scope);  
    constraints.setCountLimit(limit);
    constraints.setTimeLimit(timeout);
    constraints.setReturningAttributes(attrToReturn);
    
    return constraints;      
  }


  /**    */
  private Vector convertSearchResultsV(NamingEnumeration results, String base) throws NamingException {
    Vector rs = new Vector();

    SearchResult si;
    MDSResult mdsResult;
    String name;

    while (results != null && results.hasMoreElements()) {
      si   = (SearchResult)results.next(); 
      mdsResult   = convertAttributes(si.getAttributes());
      name = si.getName().trim();

      if (name.length() == 0) 
	mdsResult.add("dn", base);
      else
	mdsResult.add("dn", name + ", " + base);

      rs.addElement(mdsResult);
    }

    return rs;
  }

  /**    */
  private Hashtable convertSearchResultsH(NamingEnumeration results, String base) throws NamingException {
    Hashtable rs = new Hashtable();
    
    SearchResult si;
    MDSResult mdsResult;
    String name;

    while (results != null && results.hasMoreElements()) {
      si   = (SearchResult)results.next(); 
      mdsResult   = convertAttributes(si.getAttributes());
      name = si.getName().trim();

      if (name.length() == 0) 
	rs.put(base, mdsResult);
      else
	rs.put(name + ", " + base, mdsResult);
    }

    return rs;
  }

  /**    */
  private MDSResult convertAttributes(Attributes attrs) throws NamingException {
    String attribute;
    Object [] values;
    MDSResult mdsResult = new MDSResult();
    int i;

    for (NamingEnumeration ae = attrs.getAll(); ae.hasMoreElements();) {
      BasicAttribute attr = (BasicAttribute)ae.next();
      
      attribute = attr.getID();
      values = new Object[ attr.size() ];
      
      i = 0;
      Enumeration vals = attr.getAll();
      while(vals.hasMoreElements()) {
	values[i] = vals.nextElement();
	i++;
	}
      
      mdsResult.add(attribute, values);
    }

    return mdsResult;
  }


  /**    */
  private Attributes convertAttributes(MDSResult attrs) {
    String name;
    Vector values;
    BasicAttribute at;
    int i;

    Attributes atts = new BasicAttributes();    
    Enumeration e = attrs.keys();

    while(e.hasMoreElements()) {
      name   = (String)e.nextElement();
      values = attrs.get(name);

      if (name.equals("dn")) continue;

      at = new BasicAttribute(name);
      for (i=0;i<values.size();i++) {
	at.add( values.elementAt(i) );	
      }

      atts.put(at);      
    }
    
    return atts;
  }

  
  // ############################################################
  // LDAP MODIFICATION FUNCTIONS
  // ############################################################

  /** deletes a specific object specified by the dn.
   * 
   * @throws an MDSexception if something goes bad
   * @param dn the distinguished name of the object to be deleted.
   */
  public void deleteEntry (String dn) throws MDSException {
    try { 
      ctx.destroySubcontext (dn);
    } catch (NamingException e) {
      throw new MDSException("Failed to delete.", e);
    }
  }  

  /** renames an object in an LDAP database.
   * 
   * @throws an MDSexception if something goes bad
   * @param oldDN 
   * @param newDN 
   */
  public void renameEntry (String oldDN, String newDN) throws MDSException {        
    renameEntry(oldDN, newDN, false);
  }  

  /** renames an object in an LDAP database.
   * the attribute occurs multiple times only the first one will be deleted.
   * 
   * @param oldDN 
   * @param newDN 
   * @param deleteOldDN deletes old dn from the entry
   */
  public void renameEntry (String oldDN, String newDN, boolean deleteOldDN) throws MDSException {    
    env.put("java.naming.ldap.deleteRDN", String.valueOf(deleteOldDN));
    try { 
      ctx.rename (oldDN, newDN);
    } catch (NamingException e) {
      throw new MDSException("Failed to rename.", e);
    }
  }  

  /** updates specifed dn with attributes
   * 
   * @throws an MDSexception if something goes bad
   * @param dn the distinguished name of the object.
   * @param at 
   */
  public void updateEntry (String dn, MDSResult at) throws MDSException {
    try {
      Attributes ats = convertAttributes(at);
      ctx.modifyAttributes(dn, DirContext.REPLACE_ATTRIBUTE, ats);
    } catch(NamingException e) {      
      throw new MDSException("Failed to add attributes for " + dn, e);
    } 
  }

  /** adds attributes specified in a Attributes to an Object.
   * 
   * @throws an MDSexception if something goes bad
   * @param dn the distinguished name of the object.
   * @param at 
   */
  public void addEntry (String dn, MDSResult at) throws MDSException {
    try { 
      Attributes ats = convertAttributes(at);
      ctx.createSubcontext (dn, ats);
    } catch (NamingException e) {
      throw new MDSException("Failed to add new entry", e);
    }
  }  


  // ********************************************************************************


  /** deletes a specific attribute of an entry. It removes the whole attribute with all its values.
   * 
   * @throws an MDSexception if something goes bad
   * @param dn the distinguished name of the entry
   * @param attr the attribute name of the attribute to be deleted.
   */
  public void deleteAttribute(String dn, String attr) throws MDSException {
    try {
      BasicAttributes ba = new BasicAttributes();
      ba.put( new BasicAttribute(attr) );

      ctx.modifyAttributes(dn, DirContext.REMOVE_ATTRIBUTE, ba);
    } catch(NamingException e) {      
      throw new MDSException("Failed to delete attributes for " + dn, e);
    }
  }  
   
  /** deletes attributes of an entry.
   * 
   * @throws an MDSexception if something goes bad
   * @param dn the distinguished name of the entry.
   * @param at a set of attributes to delete
   */
  public void deleteValues (String dn, MDSResult at) throws MDSException {
    try {
      Attributes ats = convertAttributes(at);
      ctx.modifyAttributes(dn, DirContext.REMOVE_ATTRIBUTE, ats);
    } catch(NamingException e) {      
      throw new MDSException("Failed to delete attributes for " + dn, e);
    }
  }  


  /** updates attributes of an entry.
   * 
   * @throws an MDSexception if something goes bad
   * @param dn the distinguished name of the entry.
   * @param at a set of attributes to replace or add
   */
  public void updateAttribute (String dn, MDSResult at) throws MDSException {
    try {
      Attributes ats = convertAttributes(at);
      ctx.modifyAttributes(dn, DirContext.REPLACE_ATTRIBUTE, ats);
    } catch(NamingException e) {      
      throw new MDSException("Failed to update attributes for " + dn, e);
    }
  }  

  /** adds attributes to an entry.
   * 
   * @throws an MDSexception if something goes bad
   * @param dn the distinguished name of the object.
   * @param at 
   */
  public void addAttribute (String dn, MDSResult at) throws MDSException {
    try {
      Attributes ats = convertAttributes(at);
      ctx.modifyAttributes(dn, DirContext.ADD_ATTRIBUTE, ats);
    } catch(NamingException e) {      
      throw new MDSException("Failed to add attributes for " + dn, e);
    }
  }  


  // ******************************************************************************
  
  /** deletes a subtree starting from dn
   * 
   * @throws an MDSexception if something goes bad
   * @param dn the distinguished name of the object.
   * @param deleteItSelf if set to true, it also deletes the dn
   */
  public void deleteTree(String dn, boolean deleteItSelf) throws MDSException {
    Vector entries = new Vector();
    MDSResult mdsResult;
    String mdsResultDN;
    
    try {
      SearchControls constraints = new SearchControls();
      constraints.setSearchScope(SUBTREE_SCOPE);  
      constraints.setReturningAttributes(new String[] {"objectclass"});
      
      NamingEnumeration results = ctx.search(dn, "(objectclass=*)", constraints);

      entries = convertSearchResultsV(results, dn);

      if (entries.size() > 0 && !deleteItSelf) entries.removeElementAt(0);

      for (int i=entries.size()-1;i>=0;i--) {
	mdsResult   = (MDSResult)entries.elementAt(i);
	mdsResultDN = (String)mdsResult.getFirstValue("dn");

        ctx.destroySubcontext (mdsResultDN);
      }
      
    } catch (NamingException e) {
      throw new MDSException("Failed to delete tree.", e);
    }
  }

  
}

