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
package org.globus.gsi.bc;

import java.io.IOException;
import java.util.Enumeration;

import org.bouncycastle.asn1.DERObjectIdentifier;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.DERSet;
import org.bouncycastle.asn1.DEREncodableVector;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.ASN1Set;
import org.bouncycastle.asn1.DERString;
import org.bouncycastle.asn1.DERPrintableString;
import org.bouncycastle.asn1.x509.X509Name;

/**
 * A helper class to deal with {@link X509Name X509Name} object.
 */
public class X509NameHelper { 
 
    private ASN1Sequence seq;
   
    /**
     * Creates an instance using the specified ASN.1 sequence.
     *
     * @param seq the name sequence
     */
    public X509NameHelper(ASN1Sequence seq) {
	this.seq = seq;
    }

    /**
     * Creates an instance using existing {@link X509Name X509Name} 
     * object. 
     * This behaves like a copy constructor.
     *
     * @param name existing <code>X509Name</code> 
     */
    public X509NameHelper(X509Name name) {
	try {
	    this.seq = (ASN1Sequence)BouncyCastleUtil.duplicate(name.getDERObject());
	} catch (IOException e) {
	    throw new RuntimeException(e.getMessage());
	}
    }

    /**
     * Converts to {@link X509Name X509Name} object.
     *
     * @return the <code>X509Name</code> object.
     */
    public X509Name getAsName() {
	return new X509Name(this.seq);
    }

    /**
     * Appends the specified OID and value pair
     * name component to the end of the current
     * name.
     * 
     * @param oid the name component oid, e.g.
     *         {@link X509Name#CN X509Name.CN}
     * @param value the value (e.g. "proxy")
     *
     */
    public void add(DERObjectIdentifier oid,
		    String value) {
	DEREncodableVector v = new DEREncodableVector();
	v.add(oid);
	v.add(new DERPrintableString(value));
	add(new DERSet(new DERSequence(v)));
    }

    /**
     * Appends the specified name component
     * entry to the current name.
     * This can be used to add handle multiple
     * AVAs in one name component.
     * 
     * @param entry the name component to add.
     */
    public void add(ASN1Set entry) {
	DEREncodableVector v = new DEREncodableVector();
	int size = seq.size();
	for (int i=0;i<size;i++) {
	    v.add(seq.getObjectAt(i));
	}
	v.add(entry);
	seq = new DERSequence(v);
    }
    
    /**
     * Gets the name component at specified
     * position.
     * 
     * @return the name component the specified 
     *         position.
     */
    public ASN1Set getNameEntryAt(int i) {
	return (ASN1Set)seq.getObjectAt(i);
    }
    
    /**
     * Gets the last name component in the 
     * current name.
     * 
     * @return the last name component. Null
     *         if there is none.
     */
    public ASN1Set getLastNameEntry() {
	int size = seq.size();
	return (size > 0) ? getNameEntryAt(size-1) : null;
    }

    /**
     * Gets the last name component from
     * the {@link X509Name X509Name} name.
     * 
     * @return the last name component. Null
     *         if there is none.
     */
    public static ASN1Set getLastNameEntry(X509Name name) {
	ASN1Sequence seq = (ASN1Sequence)name.getDERObject();
	int size = seq.size();
	return (size > 0) ? (ASN1Set)seq.getObjectAt(size-1) : null;
    }
    
    /**
     * Returns Globus format representation of the name.
     * It handles names with multiple AVAs.
     *
     * @param name the name to get the Globus format of.
     * @return the Globus format of the name
     */
    public static String toString(X509Name name) {
	if (name == null) {
	    return null;
	}
	return toString((ASN1Sequence)name.getDERObject());
    }

    private static String toString(ASN1Sequence seq) {
	if (seq == null) {
	    return null;
	}

	Enumeration e = seq.getObjects();
	StringBuffer buf = new StringBuffer();
        while (e.hasMoreElements()) {
            ASN1Set set = (ASN1Set)e.nextElement();
	    Enumeration ee = set.getObjects();
	    buf.append('/');
	    while (ee.hasMoreElements()) {
		ASN1Sequence s = (ASN1Sequence)ee.nextElement();
		DERObjectIdentifier oid = (DERObjectIdentifier)s.getObjectAt(0);
		String sym = (String)X509Name.OIDLookUp.get(oid);
		if (sym == null) {
		    buf.append(oid.getId());
		} else {
		    buf.append(sym);
		}
		buf.append('=');
		buf.append( ((DERString)s.getObjectAt(1)).getString());
		if (ee.hasMoreElements()) {
		    buf.append('+');
		}
	    }
	}
	
	return buf.toString();
    }

    /**
     * Returns Globus format representation of the name.
     */
    public String toString() {
	return toString(this.seq);
    }

}
