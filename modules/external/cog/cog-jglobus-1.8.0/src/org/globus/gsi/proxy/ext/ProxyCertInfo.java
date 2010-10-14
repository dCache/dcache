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
package org.globus.gsi.proxy.ext;

import java.io.IOException;

import org.globus.gsi.bc.BouncyCastleUtil;
import org.globus.util.I18n;

import org.bouncycastle.asn1.DERInteger;
import org.bouncycastle.asn1.DEREncodable;
import org.bouncycastle.asn1.DEREncodableVector;
import org.bouncycastle.asn1.DERObject;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.DERObjectIdentifier;

/**
 * Represents ProxyCertInfo extension.
 * <BR>
 * <PRE>
 * ProxyCertInfo ::= SEQUENCE {
 *    pCPathLenConstraint      INTEGER (0..MAX) OPTIONAL,
 *    proxyPolicy              ProxyPolicy }
 * </PRE>
 */
public class ProxyCertInfo 
    implements DEREncodable {

     private static I18n i18n =
            I18n.getI18n("org.globus.gsi.gssapi.errors",
                         ProxyCertInfo.class.getClassLoader());

    /** ProxyCertInfo extension OID */
    public static final DERObjectIdentifier OID 
	= new DERObjectIdentifier("1.3.6.1.5.5.7.1.14");
    public static final DERObjectIdentifier OLD_OID 
    	= new DERObjectIdentifier("1.3.6.1.4.1.3536.1.222");
    
    private DERInteger pathLenConstraint;
    private ProxyPolicy proxyPolicy;
    
    /**
     * Creates a new instance of the ProxyCertInfo extension from
     * given ASN1Sequence object.
     * 
     * @param seq ASN1Sequence object to create the instance from.
     */
    public ProxyCertInfo(ASN1Sequence seq) {
	if (seq.size() < 1) {
	    throw new IllegalArgumentException(i18n.getMessage("proxyErr25"));
	}

	int seqPos = 0;

	if (seq.getObjectAt(seqPos) instanceof DERInteger) {
	    this.pathLenConstraint = (DERInteger)seq.getObjectAt(seqPos);
	    seqPos++;
	}
	
	ASN1Sequence policy = 
	    (ASN1Sequence)seq.getObjectAt(seqPos);
	
	this.proxyPolicy = new ProxyPolicy(policy);
    }

    /**
     * Creates a new instance of the ProxyCertInfo extension.
     *
     * @param pathLenConstraint the path length constraint of the
     *        extension.
     * @param policy the policy of the extension. 
     */
    public ProxyCertInfo(int pathLenConstraint,
                         ProxyPolicy policy) {
	if (policy == null) {
	    throw new IllegalArgumentException();
	}
	this.pathLenConstraint = new DERInteger(pathLenConstraint);
	this.proxyPolicy = policy;
    }

    /**
     * Creates a new instance of the ProxyCertInfo 
     * extension with no path length constraint.
     *
     * @param policy the policy of the extension. 
     */
    public ProxyCertInfo(ProxyPolicy policy) {
	if (policy == null) {
	    throw new IllegalArgumentException();
	}
	this.pathLenConstraint = null;
	this.proxyPolicy = policy;
    }

    /**
     * Returns an instance of <code>ProxyCertInfo</code> from
     * given object.
     *
     * @param obj the object to create the instance from.
     * @return <code>ProxyCertInfo</code> instance.
     * @exception IllegalArgumentException if unable to
     *            convert the object to <code>ProxyCertInfo</code>
     *            instance.
     */
    public static ProxyCertInfo getInstance(Object obj) {
	if (obj instanceof ProxyCertInfo) {
	    return (ProxyCertInfo)obj;
	} else if (obj instanceof ASN1Sequence) {
            return new ProxyCertInfo((ASN1Sequence)obj);
        } else if (obj instanceof byte[]) {
	    DERObject derObj = null;
	    try {
		derObj = BouncyCastleUtil.toDERObject((byte[])obj);
	    } catch (IOException e) {
		throw new IllegalArgumentException(i18n.getMessage("proxyErr26") +
						   e.getMessage());
	    }
	    if (derObj instanceof ASN1Sequence) {
		return new ProxyCertInfo((ASN1Sequence)derObj);
	    }
	}
        throw new IllegalArgumentException(i18n.getMessage("prxoyErr27"));
    }
    
    /**
     * Returns the DER-encoded ASN.1 representation of the
     * extension.
     *
     * @return <code>DERObject</code> the encoded representation
     *         of the extension.
     */
    public DERObject getDERObject() {
	DEREncodableVector  vec = new DEREncodableVector();

        if (this.pathLenConstraint != null) {
            vec.add(this.pathLenConstraint);
        }
	
	vec.add(this.proxyPolicy.getDERObject());
	
        return new DERSequence(vec);
    }

    /**
     * Returns the policy object in the proxy.
     *
     * @return <code>ProxyPolicy</code> the policy object
     */
    public ProxyPolicy getProxyPolicy() {
	return this.proxyPolicy;
    }

    /**
     * Returns the maximum depth of the path of proxy certificates
     * that can be signed by this proxy certificate. 
     *
     * @return the maximum depth of the path of proxy certificates
     *         that can be signed by this proxy certificate. If 0 then
     *         this certificate must not be used to sign a proxy 
     *         certificate. If the path length constraint field is not
     *         defined <code>Integer.MAX_VALUE</code> is returned.
     */
    public int getPathLenConstraint() {
	if (this.pathLenConstraint != null) {
	    return this.pathLenConstraint.getValue().intValue();
	}
	return Integer.MAX_VALUE;
    }
    
}

