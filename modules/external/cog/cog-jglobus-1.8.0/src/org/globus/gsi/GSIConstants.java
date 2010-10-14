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
package org.globus.gsi;

/** 
 * Defines common constants used by GSI.
 */
public interface GSIConstants {
    
    /** The character sent on the wire to request delegation */
    public static final char DELEGATION_CHAR = 'D';

    /** Null ciphersuite supported in older Globus servers */
    public static final String[] GLOBUS_CIPHER  = {"SSL_RSA_WITH_NULL_MD5"};

    /** Indicates no delegation */
    public static final int DELEGATION_NONE = 1;

    /** Indicates limited delegation. 
     * Depending on the settings it might mean GSI-2 limited delegation
     * or GSI-3 limited delegation. */
    public static final int DELEGATION_LIMITED = 2;

    /** Indicates full delegation. 
     * Depending on the settings it might mean GSI-2 full delegation
     * or GSI-3 impersonation delegation. */
    public static final int DELEGATION_FULL = 3;

    /** Indicates GSI mode (allows for delegation during authentication). 
     */
    public static final Integer MODE_GSI = new Integer(1);
    
    /** Indicates SSL compatibility mode (does not allow for delegation 
     * during authentication). */
    public static final Integer MODE_SSL = new Integer(2);
    
    /** Indicates full delegation. */
    public static final Integer DELEGATION_TYPE_FULL 
	= new Integer(GSIConstants.DELEGATION_FULL);
    
    /** Indicates limited delegation. */
    public static final Integer DELEGATION_TYPE_LIMITED 
	= new Integer(GSIConstants.DELEGATION_LIMITED);
    
    /** Indicates End-Entity Certificate, e.g. user certificate */
    public static final int EEC = 3;

    /** Indicates Certificate Authority certificate */
    public static final int CA = 4;
    
    /** Indicates legacy full Globus proxy */
    public static final int GSI_2_PROXY         = 10;

    /** Indicates legacy limited Globus proxy */
    public static final int GSI_2_LIMITED_PROXY = 11;

    /** Indicates proxy draft compliant restricted proxy.
     * A proxy with embedded policy. */
    public static final int GSI_3_RESTRICTED_PROXY    = 12;

    /** Indicates proxy draft compliant independent proxy.
     * A proxy with {@link org.globus.gsi.proxy.ext.ProxyPolicy#INDEPENDENT
     * ProxyPolicy.INDEPENDENT} policy language OID.*/
    public static final int GSI_3_INDEPENDENT_PROXY   = 13;

    /** Indicates proxy draft compliant impersonation proxy.
     * A proxy with {@link org.globus.gsi.proxy.ext.ProxyPolicy#IMPERSONATION 
     * ProxyPolicy.IMPERSONATION} policy language OID.*/
    public static final int GSI_3_IMPERSONATION_PROXY = 14;

    /** Indicates proxy draft compliant limited impersonation proxy.
     * A proxy with {@link org.globus.gsi.proxy.ext.ProxyPolicy#LIMITED 
     * ProxyPolicy.LIMITED} policy language OID.*/
    public static final int GSI_3_LIMITED_PROXY       = 15;

    /** Indicates RFC 3820 compliant restricted proxy.
     * A proxy with embedded policy. */
    public static final int GSI_4_RESTRICTED_PROXY    = 16;

    /** Indicates RFC 3820 compliant independent proxy.
     * A proxy with {@link org.globus.gsi.proxy.ext.ProxyPolicy#INDEPENDENT
     * ProxyPolicy.INDEPENDENT} policy language OID.*/
    public static final int GSI_4_INDEPENDENT_PROXY   = 17;

    /** Indicates RFC 3820 compliant impersonation proxy.
     * A proxy with {@link org.globus.gsi.proxy.ext.ProxyPolicy#IMPERSONATION 
     * ProxyPolicy.IMPERSONATION} policy language OID.*/
    public static final int GSI_4_IMPERSONATION_PROXY = 18;

    /** Indicates RFC 3820 compliant limited impersonation proxy.
     * A proxy with {@link org.globus.gsi.proxy.ext.ProxyPolicy#LIMITED 
     * ProxyPolicy.LIMITED} policy language OID.*/
    public static final int GSI_4_LIMITED_PROXY       = 19;

    /** GSI Transport protection method type
     * that will be used or was used to protect the request.
     * Can be set to:
     * {@link GSIConstants#SIGNATURE SIGNATURE} or
     * {@link GSIConstants#ENCRYPTION ENCRYPTION} or
     * {@link GSIConstants#NONE NONE}.
     */
    public static final String GSI_TRANSPORT =
        "org.globus.security.transport.type";

    /** integrity message protection method. */
    public static final Integer SIGNATURE
        = new Integer(1);

    /** privacy message protection method. */
    public static final Integer ENCRYPTION
        = new Integer(2);

    /** none message protection method. */
    public static final Integer NONE =
        new Integer(Integer.MAX_VALUE);

    /**
     * It is used to set a list of trusted certificates
     * to use during authentication (by default, the trusted certificates
     * are loaded from a standard location) The value is an instance of
     * {@link org.globus.gsi.TrustedCertificates TrustedCertificates}
     */
    public static final String TRUSTED_CERTIFICATES = 
        "org.globus.security.trustedCertifictes";

    /** 
     * It is set to a Boolean value and if false,
     * client authorization requirement with delegation is disabled. By
     * default, client side authorization (to authorize the server) is
     * required for delegation of credentials.
     */
    public static final String AUTHZ_REQUIRED_WITH_DELEGATION = 
        "org.globus.security.authz.required.delegation";
}
