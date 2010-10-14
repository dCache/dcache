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
package org.globus.common;

import java.util.Properties;
import java.util.Enumeration;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.globus.util.ConfigUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Responsible for managing the properties file 
 * "~/.globus/cog.properties", which holds information about various properties
 * needed by the security classes.  These properties include:
 * <UL>
 * <LI> the location of the user certificate file </LI>
 * <LI> the location of the user key file </LI>
 * <LI> the location of the CA certificates </LI>
 * <LI> the location of the proxy file </LI>
 * <LI> the tcp port range </LI>
 * <LI> the local ip address for DHCP systems</LI>
 * <LI> the socket timeout when connecting to a myproxy host </LI>
 * </UL>
 */
public class CoGProperties extends Properties {

    private static Log logger =
        LogFactory.getLog(CoGProperties.class.getName());
    
    private static final String DEFAULT_RANDOM_PROVIDER =
	"cryptix.jce.provider.CryptixRandom";
    
    private static final String DEFAULT_RANDOM_ALGORITHM = 
	"DevRandom";

    public static final String ENFORCE_SIGNING_POLICY = 
        "java.security.gsi.signing.policy";

    public static final String DELEGATION_KEY_CACHE_LIFETIME =
        "org.globus.jglobus.delegation.cache.lifetime";
    
    public static final String CRL_CACHE_LIFETIME =
        "org.globus.jglobus.crl.cache.lifetime";

    public static final String MDSHOST = "localhost";
    public static final String MDSPORT = "2135";
    public static final String BASEDN  = "Mds-Vo-name=local, o=Grid";
    
    /** the configuration file properties are read from -- 
     * located in ~/.globus" */
    public static final String CONFIG_FILE = "cog.properties";
    
    /** the default properties file **/
    private static CoGProperties defaultProps = null;
    
    /** the config file location **/
    public static String configFile = null;
    
    public CoGProperties() {
    }
    
    public CoGProperties(String file) 
	throws IOException {
	load(file);
    }

    public synchronized static CoGProperties getDefault() {
	if (defaultProps != null) {
	    return defaultProps;
	}
	
	defaultProps = new CoGProperties();
	
	String file = System.getProperty("org.globus.config.file");
	if (file == null) {
	    file = ConfigUtil.globus_dir + CONFIG_FILE;
	} else if (file.equalsIgnoreCase("none")) {
	    return defaultProps;
	}

	configFile = file;
	
	try {
	    defaultProps.load(configFile);
	} catch(IOException e) {
	    logger.debug("Failed to load " + CONFIG_FILE + ". Using defaults.", e);
	}
	
	return defaultProps;
    }

    /**
     * Sets default configuration. It can be used
     * to set a different configuration dynamically.
     */
    public static void setDefault(CoGProperties properties) {
	defaultProps = properties;
    }
    
    public void save()
	throws IOException {
	save(configFile);
    }

    public void save(String file) 
	throws IOException {
	OutputStream out = null;
	try {
	    out = new FileOutputStream(file);
	    store(out, "Java CoG Kit Configuration File");
	} finally {
	    if (out != null) {
		try { out.close(); } catch (Exception e) {}
	    }
	}
    }
    
    public void load(String file) 
	throws IOException {
	FileInputStream in = null;
	try {
	    in = new FileInputStream(file);
	    load(in);
	} finally {
	    if (in != null) {
		try { in.close(); } catch(Exception e) {}
	    }
	}
    }
    
    public void load(InputStream in) 
	throws IOException {
	super.load(in);
	fixSpace(this);
    }
    
    public static void fixSpace(Properties p) {
	// this will get rid off the trailing spaces
	String key, value;
	Enumeration e = p.keys();
	while(e.hasMoreElements()) {
	    key   = e.nextElement().toString();
	    value = p.getProperty(key);
	    p.put(key, value.trim());
	}
    }

    /**
     * Retrieves the location of the user cert file. 
     * It first checks the X509_USER_CERT system property. If the property
     * is not set, it checks next the 'usercert' property in the current
     * configuration. If that property is not set, it returns a default
     * location of the user cert file. The default value
     * is the 'usercert.pem' file in the user's globus directory. For example:
     * ${user.home}/.globus/usercert.pem.
     *
     * @return <code>String</code> the location of the user cert file
     */
    public String getUserCertFile() {
	String location;
	location = System.getProperty("X509_USER_CERT");
	if (location != null) {
	    return location;
	}
	location = getProperty("usercert");
	if (location != null) {
	    return location;
	}
	return ConfigUtil.discoverUserCertLocation();
    }

    public void setUserCertFile(String userCertFile) {
	put("usercert", userCertFile);
    }

    public String getPKCS11LibraryName() {
	String lib;
	lib = System.getProperty("PKCS11_LIB");
	if (lib != null) {
	    return lib;
	}
	lib = getProperty("pkcs11lib");
	if (lib != null) {
	    return lib;
	}
	return ConfigUtil.discoverPKCS11LibName();
    }
  
    public String getDefaultPKCS11Handle() {
	return getProperty("pkcs11.handle", "Globus User Credentials");
    }

    /**
     * Retrieves the location of the user key file.  
     * It first checks the X509_USER_KEY system property. If the property
     * is not set, it checks next the 'userkey' property in the current
     * configuration. If that property is not set, it returns a default
     * location of the user key file. The default value
     * is the 'userkey.pem' file in the user's globus directory. For example:
     * ${user.home}/.globus/userkey.pem.
     *
     * @return <code>String</code> the location of the user key file
     */
    public String getUserKeyFile() {
	String location;
	location = System.getProperty("X509_USER_KEY");
	if (location != null) {
	    return location;
	}
	location = getProperty("userkey");
	if (location != null) {
	    return location;
	}
	return ConfigUtil.discoverUserKeyLocation();
    }

    /**
     * Sets user key file location
     * @param userKeyFile user key file location
     */
    public void setUserKeyFile(String userKeyFile) {
	put("userkey", userKeyFile);
    }
    
    /**
     * Returns the user specified hostname. This is used
     * for DHCP machines where java is unable to determine the
     * right hostname/IP address.
     * It first checks the 'GLOBUS_HOSTNAME' system property. If the property
     * is not set, it checks the 'host' system property next. If the 'host' 
     * property is not set in the current configuration, null is returned
     * (and default 'localhost' hostname will be used)
     *
     * @return <code>String</code> the hostname of the machine.
     */
    public String getHostName() {
	String value = System.getProperty("GLOBUS_HOSTNAME");
	if (value != null) {
	    return value;
	}
	return getProperty("hostname", null);
    }

    /**
     * Sets hostname
     * @param host hostname
     */
    public void setHostName(String host) {
	put("hostname", host);
    }

    /**
     * Returns the user specified ip address. This is used
     * for DHCP machines where java is unable to determine the
     * right IP address.
     * It first checks the 'org.globus.ip' system property.
     * If that property is not set, it checks next the 'ip' property 
     * in the current configuration. If the 'ip' property is not set in the
     * current configuration, the hostname of the machine is looked up
     * using the {@link #getHostName() getHostName()} function. If 
     * <code>getHostName()</code> returns a hostname that hostname is converted
     * into an IP address and it is returned. Otherwise, null is returned
     * (and default ip address will be used)
     *
     * @return <code>String</code> the ip address of the machine.
     */
    public String getIPAddress() {
	String value = System.getProperty("org.globus.ip");
	if (value != null) {
	    return value;
	}
	value = getProperty("ip", null);
	if (value != null) {
	    return value;
	}
	value = getHostName();
	if (value != null) {
	    try {
		return InetAddress.getByName(value).getHostAddress();
	    } catch (UnknownHostException e) {
		return null;
	    }
	}
	return value;
    }

    /**
     * Sets ip address
     * @param ipAddress ip address
     */
    public void setIPAddress(String ipAddress) {
	put("ip", ipAddress);
    }
    
    /**
     * @deprecated Use getCaCertLocations() instead.
     *
     * @see #getCaCertLocations() getCaCertLocations
     *
     * @return <code>String</code> the locations of the CA certificates
     */
    public String getCaCertFile() {
	return getCaCertLocations();
    }
    
    /**
     * @deprecated Use getCaCertLocations() instead.
     *
     * @see #getCaCertLocations() getCaCertLocations
     *
     * @return <code>String</code> the locations of the CA certificates
     */
    public String getCaCerts() {
	return getCaCertLocations();
    }
    
    /**
     * Retrieves the location of the CA certificate files.  
     * It first checks the X509_CERT_DIR system property. If the property
     * is not set, it checks next the 'cacert' property in the current
     * configuration. If that property is not set, it tries to find
     * the certificates using the following rules:<BR>
     * First the ${user.home}/.globus/certificates directory is checked.
     * If the directory does not exist, and on a Unix machine, the
     * /etc/grid-security/certificates directory is checked next.
     * If that directory does not exist and GLOBUS_LOCATION 
     * system property is set then the ${GLOBUS_LOCATION}/share/certificates
     * directory is checked. Otherwise, null is returned. 
     * This indicates that the certificates directory could
     * not be found.
     * <BR>
     * Moreover, this function can return multiple file and directory 
     * locations. The locations must be comma separated.
     *
     * @return <code>String</code> the locations of the CA certificates
     */
    public String getCaCertLocations() {
	String location;
	location = System.getProperty("X509_CERT_DIR");
	if (location != null) {
	    return location;
	}
	location = getProperty("cacert");
	if (location != null) {
	    return location;
	}
	return ConfigUtil.discoverCertDirLocation();
    }

    public void setCaCertLocations(String list) {
	put("cacert", list);
    }

    /**
     * Retrieves the location of the proxy file. 
     * It first checks the X509_USER_PROXY system property. If the property
     * is not set, it checks next the 'proxy' property in the current
     * configuration. If that property is not set, then it defaults to a 
     * value based on the following rules: <BR>
     * If a UID system property is set, and running on a Unix machine it 
     * returns /tmp/x509up_u${UID}. If any other machine then Unix, it returns
     * ${tempdir}/x509up_u${UID}, where tempdir is a platform-specific 
     * temporary directory as indicated by the java.io.tmpdir system property. 
     * If a UID system property is not set, the username will be used instead
     * of the UID. That is, it returns ${tempdir}/x509up_u_${username}
     * <BR>
     * This is done this way because Java is not able to obtain the current 
     * uid.
     *
     * @return <code>String</code> the location of the proxy file
     */
    public String getProxyFile() {
	String location;
	location = System.getProperty("X509_USER_PROXY");
	if (location != null) {
	    return location;
	}
	location = getProperty("proxy");
	if (location != null) {
	    return location;
	}
	return ConfigUtil.discoverProxyLocation();
    }

    public void setProxyFile(String proxyFile) {
	put("proxy", proxyFile);
    }
    
    /**
     * Returns the tcp port range.
     * It first checks the 'GLOBUS_TCP_PORT_RANGE' system property. If that 
     * system property is not set then 'org.globus.tcp.port.range' system
     * property is checked. If that system property is not set then it returns
     * the value specified in the configuration file. Returns null if the port
     * range is not defined.<BR>
     * The port range is in the following form: <minport>, <maxport>
     *
     * @return <code>String</code> the port range. 
     */
    public String getTcpPortRange() {
	String value = null;
	value = System.getProperty("GLOBUS_TCP_PORT_RANGE");
	if (value != null) {
	    return value;
	}
	value = System.getProperty("org.globus.tcp.port.range");
	if (value != null) {
	    return value;
	}
	return getProperty("tcp.port.range", null);
    }

    /**
     * Returns the tcp source port range.
     * It first checks the 'GLOBUS_TCP_SOURCE_PORT_RANGE' system property. 
     * If that system property is not set then 
     * 'org.globus.source.tcp.port.range' system property is checked. 
     * If that system property is not set then it returns
     * the value specified in the configuration file. Returns null if the port
     * range is not defined.<BR>
     * The port range is in the following form: <minport>, <maxport>
     *
     * @return <code>String</code> the port range. 
     */
    public String getTcpSourcePortRange() {
	String value = null;
	value = System.getProperty("GLOBUS_TCP_SOURCE_PORT_RANGE");
	if (value != null) {
	    return value;
	}
	value = System.getProperty("org.globus.tcp.source.port.range");
	if (value != null) {
	    return value;
	}
	return getProperty("tcp.source.port.range", null);
    }

    /**
     * Returns the udp source port range.
     * It first checks the 'GLOBUS_UDP_SOURCE_PORT_RANGE' system property. 
     * If that system property is not set then 
     * 'org.globus.source.udp.port.range' system property is checked. 
     * If that system property is not set then it returns
     * the value specified in the configuration file. Returns null if the port
     * range is not defined.<BR>
     * The port range is in the following form: <minport>, <maxport>
     *
     * @return <code>String</code> the port range. 
     */
    public String getUdpSourcePortRange() {
	String value = null;
	value = System.getProperty("GLOBUS_UDP_SOURCE_PORT_RANGE");
	if (value != null) {
	    return value;
	}
	value = System.getProperty("org.globus.udp.source.port.range");
	if (value != null) {
	    return value;
	}
	return getProperty("udp.source.port.range", null);
    }
    
    /**
     * Returns whether to use the /dev/urandom device
     * for seed generation.
     *
     * @return true if the device should be used (if available of course)
     *         Returns true by default unless specified otherwise by the
     *         user.
     */
    public boolean useDevRandom() {
	String value = System.getProperty("org.globus.dev.random");
	if (value != null && value.equalsIgnoreCase("no")) {
	    return false;
	}
	return getAsBoolean("org.globus.dev.random", true);
    }

    public boolean enforceSigningPolicy() {

	String value = System.getProperty(ENFORCE_SIGNING_POLICY);
	if ((value != null) && (value.equalsIgnoreCase("no")
                              || (value.equalsIgnoreCase("false")))) {
	    return false;
	}
	return getAsBoolean(ENFORCE_SIGNING_POLICY, true);
    }

    /**
     * Returns the delegation key cache lifetime for all delegations from this
     * JVM. If this property is not set or set to zero or less, no caching is done. The
     * value is the number of milliseconds the key/pair is cached.
     * @return
     */
    public int getDelegationKeyCacheLifetime() {

        int valueInt = 0;

        String valueStr = System.getProperty(DELEGATION_KEY_CACHE_LIFETIME);

        if (valueStr != null && valueStr.length() > 0) {
            int parsedvalueInt = Integer.parseInt(valueStr);
            if (parsedvalueInt > 0) {
                valueInt = parsedvalueInt;
            }
        }

        if (valueInt == -1) { // Didn't find a system property
            valueStr = getProperty(DELEGATION_KEY_CACHE_LIFETIME);
            if (valueStr != null && valueStr.length() > 0) {
                int parsedvalueInt = Integer.parseInt(valueStr);
                if (parsedvalueInt > 0) {
                    valueInt = parsedvalueInt;
                }
            }
        }
        return valueInt;
    }


    /**
     * Returns the CRL cache lifetime. If this property is not set or
     * set to zero or less, no caching is done. The value is the
     * number of milliseconds the CRLs are cached without checking for
     * modifications on disk.
     *
     * @throws NumberFormatException if the cache lifetime property
     *         could not be parsed
     * @return the CRL cache lifetime in milliseconds
     */
    public long getCRLCacheLifetime()
        throws NumberFormatException {

        long value = 0;

        String property = getProperty(CRL_CACHE_LIFETIME);
        if (property != null && property.length() > 0) {
            long parsedValue  = Long.parseLong(property);
            if (parsedValue > 0) {
                value = parsedValue;
            }
        }

        // System property takes precedence
        property = System.getProperty(CRL_CACHE_LIFETIME);
        if (property != null && property.length() > 0) {
            long parsedValue = Long.parseLong(property);
            if (parsedValue > 0) {
                value = parsedValue;
            }
        }

        return value;
    }

    public String getSecureRandomProvider() {
	String value = System.getProperty("org.globus.random.provider");
	if (value != null) {
	    return value;
	}
	return getProperty("random.provider", 
			   DEFAULT_RANDOM_PROVIDER);
    }

    public String getSecureRandomAlgorithm() {
	String value = System.getProperty("org.globus.random.algorithm");
	if (value != null) {
	    return value;
	}
	return getProperty("random.algorithm", 
			   DEFAULT_RANDOM_ALGORITHM);
    }

    /**
     * Returns the timeout (in seconds) for creating a new socket connection
     * to a MyProxy host.  The socket timeout property can be set either as
     * the Java system property "MYPROXY_SOCKET_TIMEOUT" (i.e. via the '-D'
     * command line option or environment variable) or via the
     * "sockettimeout" property in the cog.properties file.  If no such
     * property is found, the default timeout of 10 seconds is returned.
     *
     * @return The timeout for creating a socket connectino to a MyProxy
     *         host. Defaults to 10 seconds.
     */
    public int getSocketTimeout() {
        int timeoutInt = -1;  // -1 indicates it hasn't been set yet
        String timeoutStr = System.getProperty("MYPROXY_SOCKET_TIMEOUT");
        if (timeoutStr != null && timeoutStr.length() > 0) {
            int parsedTimeoutInt = Integer.parseInt(timeoutStr);
            if (parsedTimeoutInt >= 0) {
                timeoutInt = parsedTimeoutInt;
            }
        }
        if (timeoutInt == -1) { // Didn't find a system property
            timeoutStr = getProperty("sockettimeout");
            if (timeoutStr != null && timeoutStr.length() > 0) {
                int parsedTimeoutInt = Integer.parseInt(timeoutStr);
                if (parsedTimeoutInt >= 0) {
                    timeoutInt = parsedTimeoutInt;
                }
            }
        }
        if (timeoutInt == -1) { // Didn't find any property at all
            timeoutInt = 10;
        }
        return timeoutInt;
    }

    public void setSocketTimeout(int socketTimeout) {
        put("sockettimeout", String.valueOf(socketTimeout));
    }


    // -------------------------------------------------------
    
    public int getProxyStrength() {
	return getAsInt("proxy.strength", 512);
    }
    
    public void setProxyStrength(int strength) {
	put("proxy.strength", String.valueOf(strength));
    }
    
    public int getProxyLifeTime() {
	return getAsInt("proxy.lifetime", 12);
    }
    
    public void setProxyLifeTime(int lifeTimeInHours) {
	put("proxy.lifetime", String.valueOf(lifeTimeInHours));
    }
    
    // --------------------------------------------------------

    // --- Most of these functions are deprecated ---

    public String getRootMDSHost() {
	return getProperty("mds.root.host", MDSHOST);
    }
    
    public String getRootMDSPort() {
	return getProperty("mds.root.port", MDSPORT);
    }
    
    public String getRootMDSBaseDN() {
	return getProperty("mds.root.basedn", BASEDN);
    }
    
    public String getOrgMDSHost() {
	return getProperty("mds.org.host", MDSHOST);
    }
    
    public String getOrgMDSPort() {
	return getProperty("mds.org.port", MDSPORT);
    }
    
    public String getOrgMDSBaseDN() {
	return getProperty("mds.org.basedn", BASEDN);
    }
    
    // ----------------------------------------------------
    
    protected boolean getAsBoolean(String key, boolean defaultValue) {
	String tmp = getProperty(key);
	if (tmp == null) {
	    return defaultValue;
	}
	return (tmp.equalsIgnoreCase("yes") || tmp.equalsIgnoreCase("true"));
    }
    
    protected int getAsInt(String label, int defValue) {
	String tmp = getProperty(label);
	return (isNullOrEmpty(tmp)) ? defValue : Integer.parseInt(tmp);
    }
    
    protected final static boolean isNullOrEmpty(String tmp) {
	return (tmp == null || (tmp != null && tmp.length() == 0));
    }
    
}
