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
package org.gridforum.jgss;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

/**
 * Defines Java API for credential export extension as defined in the 
 * <a href="http://www.gridforum.org/security/gsi/draft-ggf-gss-extensions-08.pdf">GSS-API Extensions document</a>.
 * Some of the functions might not specify all the parameters as in the document. 
 * <BR><BR>Notes:
 * <UL>
 * <LI>Protection key is currently not supported.</LI>
 * </UL>
 */
public interface ExtendedGSSCredential extends GSSCredential {   
    
    public static final int 
	IMPEXP_OPAQUE = 0,
	IMPEXP_MECH_SPECIFIC = 1;

    /**
     * Exports this credential so that another process might import it. 
     * The exported credential might be imported again using the 
     * {@link ExtendedGSSManager#createCredential(byte[], int, int, Oid, int) 
     * ExtendedGSSManager.createCredential} method.
     * 
     * @param option 
     *        The export type. If set to {@link ExtendedGSSCredential#IMPEXP_OPAQUE
     *        ExtendedGSSCredential.IMPEXP_OPAQUE} exported buffer is an opaque
     *        buffer suitable for storage in memory or on disk or passing to
     *        another process. If set to {@link ExtendedGSSCredential#IMPEXP_MECH_SPECIFIC 
     *        ExtendedGSSCredential.IMPEXP_MECH_SPECIFIC} exported buffer is a 
     *        buffer filled with mechanism-specific information that the calling
     *        application can use to pass the credential to another process that 
     *        is not written to the GSS-API.
     * @return The buffer containing the credential
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.CREDENTIAL_EXPIRED,
     *            GSSException.UNAVAILABLE, GSSException.FAILURE</code>
     */
    public byte[] export(int option)
	throws GSSException;
    
    /**
     * Exports this credential so that another process might import it. 
     * The exported credential might be imported again using the 
     * {@link ExtendedGSSManager#createCredential(byte[], int, int, Oid, int)
     * ExtendedGSSManager.createCredential} method.
     * 
     * @param option 
     *        The export type. If set to {@link ExtendedGSSCredential#IMPEXP_OPAQUE 
     *        ExtendedGSSCredential.IMPEXP_OPAQUE} exported buffer is an opaque
     *        buffer suitable for storage in memory or on disk or passing to 
     *        another process. If set to {@link ExtendedGSSCredential#IMPEXP_MECH_SPECIFIC
     *        ExtendedGSSCredential.IMPEXP_MECH_SPECIFIC} exported buffer is a buffer
     *        filled with mechanism-specific information that the calling application
     *        can use to pass the credential to another process that is not written
     *        to the GSS-API.
     * @param mech Desired mechanism for exported credential, may be null to 
     *             indicate system default.
     * @return The buffer containing the credential
     * @exception GSSException containing the following major error codes:
     *            <code>GSSException.CREDENTIAL_EXPIRED,
     *            GSSException.UNAVAILABLE, GSSException.BAD_MECH, GSSException.FAILURE</code>
     */
    public byte[] export(int option, Oid mech)
	throws GSSException;

    /**
     * Retrieves arbitrary data about this credential.
     *
     * @param oid the oid of the information desired.
     * @return the information desired. Might be null.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public Object inquireByOid(Oid oid) throws GSSException;
}
