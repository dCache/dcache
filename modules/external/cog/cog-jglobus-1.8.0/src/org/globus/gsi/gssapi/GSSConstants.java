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
package org.globus.gsi.gssapi;

import org.ietf.jgss.Oid;

/**
 * Defines common GSI-GSS constants.
 */
public abstract class GSSConstants {

    /** Globus GSI GSS mechanism Oid */
    public static final Oid MECH_OID;

    /** Context option. It is used to configure the GSS mode. It can be set to
     * {@link org.globus.gsi.GSIConstants#MODE_GSI GSIConstants.MODE_GSI} or
     * {@link org.globus.gsi.GSIConstants#MODE_SSL GSIConstants.MODE_SSL}. 
     * By default GSI mode is enabled. */
    public static final Oid GSS_MODE;

    /** Context option. It is used to enable/disable the rejection of
     * limited proxies during authentication. In can be set to either
     * <code>Boolean.TRUE</code> or <code>Boolean.FALSE</code>.
     * By default limited proxies are accepted.*/
    public static final Oid REJECT_LIMITED_PROXY;

    /** Context option. It is used to configure delegation type to be 
     * performed either during authentication or using the delegation API.
     * It can be set to {@link org.globus.gsi.GSIConstants#DELEGATION_TYPE_LIMITED
     * GSIConstants.DELEGATION_TYPE_LIMITED} or 
     * {@link org.globus.gsi.GSIConstants#DELEGATION_TYPE_FULL 
     * GSIConstants.DELEGATION_TYPE_FULL}
     * By default limited delegation is performed. */
    public static final Oid DELEGATION_TYPE;
    
    /** Context option. It is used to enable/disable context expiration
     * checking for methods like <code>wrap, unwrap, verifyMIC, getMIC</code>.
     * In can be set to either <code>Boolean.TRUE</code> or
     * <code>Boolean.FALSE</code>. By default context expiration checking
     * is disabled. */
    public static final Oid CHECK_CONTEXT_EXPIRATION;

    /** Context option. It is used to enable/disable client authentication
     * on acceptor side. In can be set to either <code>Boolean.TRUE</code>
     * or <code>Boolean.FALSE</code>. By default client authentication is
     * enabled.*/
    public static final Oid REQUIRE_CLIENT_AUTH;

    /** Context option. It is only used when client authentication is enabled.
     * In can be set to either <code>Boolean.TRUE</code> or 
     * <code>Boolean.FALSE</code>. If set to <code>Boolean.TRUE</code>
     * a context will be successfully established even though client
     * send no certificates and client authentication was required.
     * If set to <code>Boolean.FALSE</code>, the context establishment will fail
     * if client does not send its certificates and client authentication
     * was requested.
     */
    public static final Oid ACCEPT_NO_CLIENT_CERTS;

    /** Context option. It is used to set a policy handler for 
     * GRIM credentials. The value is an instance of 
     * {@link org.globus.gsi.proxy.ProxyPolicyHandler
     * ProxyPolicyHandler}
     * @deprecated Please use {@link GSSConstants#PROXY_POLICY_HANDLERS 
     * GSSConstants.PROXY_POLICY_HANDLERS} option instead.
     */
    public static final Oid GRIM_POLICY_HANDLER;

    /** Context option. It is used to pass a set of proxy policy handlers.
     * The value if a <code>Map</code> type. It contains mappings of
     * proxy policy language oids and instances of 
     * {@link org.globus.gsi.proxy.ProxyPolicyHandler ProxyPolicyHandler}
     */
    public static final Oid PROXY_POLICY_HANDLERS;

    /** Context option. It is used to set a list of trusted certificates
     * to use during authentication (by default, the trusted certificates
     * are loaded from a standard location) The value is an instance of
     * {@link org.globus.gsi.TrustedCertificates TrustedCertificates}
     */
    public static final Oid TRUSTED_CERTIFICATES;

    /** Used in inquireByOid function. Returns the certificate chain. */
    public static final Oid X509_CERT_CHAIN;

    /** Used in inquireByOid method. Retuns if peer presented a
     * limited credential
     */
    public static final Oid RECEIVED_LIMITED_PROXY;

    /** Context option. It is set to a Boolean value and if false,
     * client authorization requirement with delegation is disabled. By
     * default, client side authorization (to authorize the server) is
     * required for delegation of credentials.
     */
    public static final Oid AUTHZ_REQUIRED_WITH_DELEGATION;

    /** Quality-of-Protection (QOP) value, indicates large block size support.
     * Can be passed to <code>wrap</code> or set by <code>unwrap</code>
     * methods  */
    public static final int GSI_BIG = 1; // GSS_C_QOP_GLOBUS_GSSAPI_OPENSSL_BIG

    static {
	try {
	    // globus mech oid
	    MECH_OID = new Oid("1.3.6.1.4.1.3536.1.1");
	     
	    // options
	    GSS_MODE = new Oid("1.3.6.1.4.1.3536.1.1.1");
	    DELEGATION_TYPE = new Oid("1.3.6.1.4.1.3536.1.1.2");
	    CHECK_CONTEXT_EXPIRATION = new Oid("1.3.6.1.4.1.3536.1.1.3");
	    REJECT_LIMITED_PROXY = new Oid("1.3.6.1.4.1.3536.1.1.4");
	    REQUIRE_CLIENT_AUTH = new Oid("1.3.6.1.4.1.3536.1.1.5");
	    GRIM_POLICY_HANDLER = new Oid("1.3.6.1.4.1.3536.1.1.6");
	    TRUSTED_CERTIFICATES = new Oid("1.3.6.1.4.1.3536.1.1.7");
	    X509_CERT_CHAIN = new Oid("1.3.6.1.4.1.3536.1.1.8");
	    
	    ACCEPT_NO_CLIENT_CERTS = new Oid("1.3.6.1.4.1.3536.1.1.19");
	    PROXY_POLICY_HANDLERS = new Oid("1.3.6.1.4.1.3536.1.1.20");
	    RECEIVED_LIMITED_PROXY = new Oid("1.3.6.1.4.1.3536.1.1.21");
	    AUTHZ_REQUIRED_WITH_DELEGATION = 
                new Oid("1.3.6.1.4.1.3536.1.1.22");
	} catch (Exception e) {
	    throw new RuntimeException(e.getMessage());
	}
    }
    
}
