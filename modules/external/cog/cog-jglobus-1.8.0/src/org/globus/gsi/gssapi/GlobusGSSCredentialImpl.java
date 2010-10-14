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

import org.globus.util.Util;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import org.gridforum.jgss.ExtendedGSSCredential;

import java.security.cert.X509Certificate;
import java.security.PrivateKey;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;

import org.globus.gsi.GlobusCredential;

/**
 * An implementation of <code>GlobusGSSCredential</code>.
 */
public class GlobusGSSCredentialImpl implements ExtendedGSSCredential,
                                                Serializable {   

    private int usage = 0;
    private GlobusCredential cred;
    private GSSName name;

    /**
     * Creates anonymous credential.
     */
    public GlobusGSSCredentialImpl() {
	this.cred = null;
	this.name = new GlobusGSSName();
    }

    /**
     * Creates regular credential from specified
     * GlobusCredential object.
     *
     * @param cred the credential
     * @param usage credential usage
     */
    public GlobusGSSCredentialImpl(GlobusCredential cred,
				   int usage) 
	throws GSSException {
	if (cred == null) {
	    throw new IllegalArgumentException("cred == null");
	}

	this.cred = cred;
	this.usage = usage;
	this.name = new GlobusGSSName(cred.getIdentity());
    }

    public int hashCode() {
	if (this.cred == null) {
	    return this.usage;
	} else {
	    return this.cred.hashCode() + this.usage;
	}
    }

    public boolean equals(Object obj) {
	if (obj instanceof GlobusGSSCredentialImpl) {
	    GlobusGSSCredentialImpl other = (GlobusGSSCredentialImpl)obj;
	    return (other.usage == this.usage && other.cred == this.cred);
	}
	return false;
    }

    public void dispose() throws GSSException {
	this.cred = null;
    }
 
    public GSSName getName() throws GSSException {
	return this.name;
    }

    public GSSName getName(Oid mech) throws GSSException {
	GlobusGSSManagerImpl.checkMechanism(mech);
	return this.name;
    }

    /**
     * Currently not implemented.
     */
    public void add(GSSName aName, 
		    int initLifetime, 
		    int acceptLifetime,
		    Oid mech, 
		    int usage) 
	throws GSSException {
	// currently we are not supporting multiple mechanism
	// credentials
	throw new GSSException(GSSException.UNAVAILABLE);
    }

    public int getUsage() throws GSSException {
	return usage;
    }

    public int getUsage(Oid mech) 
	throws GSSException {
	GlobusGSSManagerImpl.checkMechanism(mech);
	return this.usage;
    }

    public int getRemainingLifetime() 
	throws GSSException {
	return (this.cred == null) ? -1 : (int)this.cred.getTimeLeft();
    }
    
    public int getRemainingInitLifetime(Oid mech) 
	throws GSSException {
	GlobusGSSManagerImpl.checkMechanism(mech);
	if (this.usage == INITIATE_ONLY ||
	    this.usage == INITIATE_AND_ACCEPT) {
	    return getRemainingLifetime();
	} else {
	    throw new GSSException(GSSException.FAILURE);
	}
    }
    
    public int getRemainingAcceptLifetime(Oid mech) 
	throws GSSException {
	GlobusGSSManagerImpl.checkMechanism(mech);
	if (this.usage == ACCEPT_ONLY ||
	    this.usage == INITIATE_AND_ACCEPT) {
	    return getRemainingLifetime();
	} else {
	    throw new GSSException(GSSException.FAILURE);
	}
    }

    public Oid[] getMechs() 
	throws GSSException {
	return GlobusGSSManagerImpl.MECHS;
    }

    public byte[] export(int option)
	throws GSSException {
	return export(option, null);
    }
    
    public byte[] export(int option, Oid mech)
	throws GSSException {
	GlobusGSSManagerImpl.checkMechanism(mech);
	if (this.cred == null) {
	    throw new GlobusGSSException(GSSException.FAILURE,
					 GlobusGSSException.CREDENTIAL_ERROR,
					 "anonCred00");
	}

	switch (option) {
	case IMPEXP_OPAQUE:
	    ByteArrayOutputStream bout = new ByteArrayOutputStream();
	    try {
		this.cred.save(bout);
	    } catch (IOException e) {
		throw new GlobusGSSException(GSSException.FAILURE, e);
	    }
	    return bout.toByteArray();
	case IMPEXP_MECH_SPECIFIC:
	    File file = null;
	    FileOutputStream fout = null;
	    try {
		file = File.createTempFile("x509up_", ".tmp");
                Util.setOwnerAccessOnly(file.getAbsolutePath());
		fout = new FileOutputStream(file);
		this.cred.save(fout);
	    } catch(IOException e) {
		throw new GlobusGSSException(GSSException.FAILURE, e);
	    } finally {
		if (fout != null) {
		    try { fout.close(); } catch (Exception e) {}
		}
	    }
	    String handle = "X509_USER_PROXY=" + file.getAbsolutePath();
	    return handle.getBytes();
	default:
	    throw new GlobusGSSException(GSSException.FAILURE, 
					 GlobusGSSException.BAD_ARGUMENT,
					 "unknownOption",
					 new Object[] {new Integer(option)});
	}
    }

    /**
     * Retrieves arbitrary data about this credential.
     * Currently supported oid: <UL>
     * <LI>
     * {@link GSSConstants#X509_CERT_CHAIN GSSConstants.X509_CERT_CHAIN}
     * returns certificate chain of this credential
     * (<code>X509Certificate[]</code>).
     * </LI>
     * </UL>
     *
     * @param oid the oid of the information desired.
     * @return the information desired. Might be null.
     * @exception GSSException containing the following major error codes: 
     *            <code>GSSException.FAILURE</code>
     */
    public Object inquireByOid(Oid oid) 
	throws GSSException {
	if (oid == null) {
	    throw new GlobusGSSException(GSSException.FAILURE, 
					 GlobusGSSException.BAD_ARGUMENT,
					 "nullOption");
	}
	
	if (oid.equals(GSSConstants.X509_CERT_CHAIN)) {
	    return (this.cred == null) ? 
		null : 
		this.cred.getCertificateChain();
	}

	return null;
    }
    
    /**
     * Returns actual GlobusCredential object represented
     * by this credential (if any).
     *
     * @return The credential object. Might be null if
     *         this is an anonymous credential.
     */
    public GlobusCredential getGlobusCredential() {
	return this.cred;
    }

    /**
     * Returns the private key of this credential (if any).
     *
     * @return The private key. Might be null if this
     *         is an anonymous credential.
     */
    public PrivateKey getPrivateKey() {
	return (this.cred == null) ? null : this.cred.getPrivateKey();
    }

    /**
     * Returns certificate chain of this credential (if any).
     *
     * @return The certificate chain. Might be null if this
     *         is an anonymous credential.
     */
    public X509Certificate [] getCertificateChain() {
	return (this.cred == null) ? null : this.cred.getCertificateChain();
    }

}
