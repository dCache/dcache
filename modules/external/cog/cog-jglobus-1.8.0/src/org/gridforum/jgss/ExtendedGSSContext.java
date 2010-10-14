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
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.Oid;

/**
 * Defines Java API for setting and getting context options and delegation extensions as defined in the 
 * <a href="http://www.gridforum.org/security/gsi/draft-ggf-gss-extensions-08.pdf">GSS-API Extensions document</a>.
 * Some of the functions might not specify all the parameters as in the document. 
 * <BR><BR>Notes:
 * <UL>
 * <LI>Extensions are not supported in initDelegation and acceptDelegation</LI>
 * </UL>
 * <BR>
 * Here is a sample code showing how the delegation API might be used: 
 * <pre>
 * ExtendedGSSContext client = ....
 * ExtendedGSSContext server = ....
 * 
 * byte [] input = new byte[0];
 * byte [] output = null;
 * do {
 *	    output = client.initDelegation(null, null, 0, input, 0, input.length);
 * 	    input = server.acceptDelegation(0, output, 0, output.length);
 * } while (!client.isDelegationFinished());
 *
 * GSSCredential cred = server.getDelegatedCredential();
 * ...
 * </pre>
 * Because delegation can be performed multiple times on the same contexts, the <code>do { ... } while ();</code>
 * block should be used to properly reset the delegation state (The state of <code>isDelegationFinished</code> 
 * is reset on the initial call to <code>initDelegation</code> or <code>acceptDelegation</code>.
 */
public interface ExtendedGSSContext extends GSSContext {
    
    /**
     * Sets a context option. It can be called by context initiator or acceptor
     * but prior to the first call to initSecContext, acceptSecContext, initDelegation
     * or acceptDelegation. 
     *
     * @param option 
     *        option type.
     * @param value 
     *        option value.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public void setOption(Oid option, Object value)
	throws GSSException;
    
    /**
     * Gets a context option. It can be called by context initiator or acceptor.
     *
     * @param option option type.
     * @return value option value. Maybe be null.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public Object getOption(Oid option) 
	throws GSSException;
    
    /**
     * Initiate the delegation of a credential.
     *
     * This functions drives the initiating side of the credential
     * delegation process. It is expected to be called in tandem with the
     * <code>acceptDelegation</code> function.
     * 
     * 
     * @param cred  
     *        The credential to be delegated. May be null
     *        in which case the credential associated with the security
     *        context is used.
     * @param mechanism
     *        The desired security mechanism. May be null.
     * @param lifetime
     *        The requested period of validity (seconds) of the delegated
     *        credential. 
     * @return A token that should be passed to <code>acceptDelegation</code> if 
     *         <code>isDelegationFinished</code> returns false. May be null.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public byte[] initDelegation(GSSCredential cred, 
				 Oid mechanism,
				 int lifetime,
				 byte[] buf, int off, int len) 
	throws GSSException;

    /**
     * Accept a delegated credential.
     *
     * This functions drives the accepting side of the credential
     * delegation process. It is expected to be called in tandem with the
     * <code>initDelegation</code> function.
     *
     * @param lifetime
     *        The requested period of validity (seconds) of the delegated
     *        credential. 
     * @return A token that should be passed to <code>initDelegation</code> if 
     *         <code>isDelegationFinished</code> returns false. May be null.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public byte[] acceptDelegation(int lifetime,
				   byte[] but, int off, int len)
	throws GSSException;
    
    /**
     * Returns the delegated credential that was delegated using
     * the <code>initDelegation</code> and <code>acceptDelegation</code>
     * functions. This is to be called on the delegation accepting
     * side once once <code>isDelegationFinished</code> returns true.
     *
     * @return The delegated credential. Might be null if credential
     *         delegation is not finished.
     */
    public GSSCredential getDelegatedCredential();
    
    /**
     * Used during delegation to determine the state of the delegation.
     *
     * @return true if delegation was completed, false otherwise.
     */
    public boolean isDelegationFinished();
 
    /**
     * Retrieves arbitrary data about this context.
     *
     * @param oid the oid of the information desired.
     * @return the information desired. Might be null.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public Object inquireByOid(Oid oid) throws GSSException;
}
