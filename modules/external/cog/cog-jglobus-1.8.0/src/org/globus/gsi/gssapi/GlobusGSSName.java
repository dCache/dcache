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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Vector;

import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

import org.globus.gsi.ptls.PureTLSUtil;

import COM.claymoresystems.cert.X509Name;

import java.io.IOException;
import java.io.Serializable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * An implementation of <code>GSSName</code>.
 */
public class GlobusGSSName implements GSSName, Serializable {

    protected Oid nameType;
    protected X509Name name;

    // set toString called
    protected String globusID;

    public GlobusGSSName() {
	this.nameType = GSSName.NT_ANONYMOUS;
	this.name = null;
    }

    public GlobusGSSName(X509Name name) {
	if (name == null) {
	    this.nameType = GSSName.NT_ANONYMOUS;
	}
	this.name = name;
    }

    public GlobusGSSName(byte[] name) {
	if (name == null) {
	    this.nameType = GSSName.NT_ANONYMOUS;
	    this.name = null;
	} else {
	    this.name = new X509Name(name);
	}
    }
    
    /**
     * Creates name from Globus DN
     *
     * @param name Globus DN (e.g. /C=US/O=Globus/..) If null
     *        it is considered set as <code>GSSName.ANONYMOUS</code> name type.
     */
    public GlobusGSSName(String name) 
	throws GSSException {
	if (name == null) {
	    this.nameType = GSSName.NT_ANONYMOUS;
	    this.name = null;
	} else {
	    try {
		this.name = PureTLSUtil.getX509Name(name);
	    } catch (Exception e) {
		throw new GlobusGSSException(GSSException.BAD_NAME, e);
	    }
	}
    }

    /**
     * Creates name from X509 name of specified type.
     *
     * @param name 
     *        Globus DN (e.g. /C=US/O=Globus/..) or service@host name. If null
     *        it is considered set as <code>GSSName.ANONYMOUS</code> name type.
     * @param nameType name type. Only <code>GSSName.NT_ANONYMOUS</code> 
     *                 or <code>GSSName.NT_HOSTBASED_SERVICE</code> is supported.
     *                 Maybe be null.
     */
    public GlobusGSSName(String name, Oid nameType)
	throws GSSException {
	if (name == null) {
	    if (nameType != null && !nameType.equals(GSSName.NT_ANONYMOUS)) {
		throw new GSSException(GSSException.BAD_NAMETYPE);
	    } 
	    this.name = null;
	    this.nameType = GSSName.NT_ANONYMOUS;
	} else {
	    if (nameType != null) {
		if (nameType.equals(GSSName.NT_HOSTBASED_SERVICE)) {
		    int atPos = name.indexOf('@');
		    if (atPos == -1 || (atPos+1 >= name.length())) {
			throw new GlobusGSSException(GSSException.FAILURE,
						     GlobusGSSException.BAD_NAME,
						     "badName00");
		    }
		    // performs reverse DNS lookup
		    String host = name.substring(atPos+1);
		    try {
			InetAddress i = InetAddress.getByName(host);
			host = InetAddress.getByName(i.getHostAddress()).getHostName();
		    } catch (UnknownHostException e) {
			throw new GlobusGSSException(GSSException.FAILURE, e);
		    }

		    String [] ava = {"CN", name.substring(0, atPos) + "/" + host};
		    Vector rdn = new Vector(1);
		    rdn.addElement(ava);
		    Vector dn = new Vector(1);
		    dn.addElement(rdn);
		    
		    this.name = new X509Name(dn);
		} else {
		    throw new GSSException(GSSException.BAD_NAMETYPE);
		}
	    } else {
		try {
		    this.name = PureTLSUtil.getX509Name(name);
		} catch (Exception e) {
		    throw new GlobusGSSException(GSSException.BAD_NAME, e);
		}
	    }
	    this.nameType = nameType;
	}
	// both subject & nameType might be null
    }
    
    public boolean isAnonymous() {
	return (this.name == null);
    }
    
    public boolean isMN() {
	return true;
    }

    public boolean equals(GSSName another)
	throws GSSException {
	if (another == null) {
	    return false;
	}

	if (isAnonymous()) {
	    return another.isAnonymous();
	} 

	if (another.isAnonymous()) {
	    return false;
	}

	if (!(another instanceof GlobusGSSName)) {
	    throw new GSSException(GSSException.FAILURE);
	}
	
	GlobusGSSName other = (GlobusGSSName)another;

	// both are not anonymous
	// both have non-null subjects
	// nametypes might be different! (null)

	if ((nameType != null && nameType.equals(GSSName.NT_HOSTBASED_SERVICE)) ||
	    (other.nameType != null && other.nameType.equals(GSSName.NT_HOSTBASED_SERVICE))) {
	    // perform host based comparison
	
	    String hp1 = this.getHostPart(true);
	    String hp2 = other.getHostPart(true);

	    if (hp1 == null || hp2 == null) {
		// something is really wrong
		return false;
	    }
	    
	    String service1 = getService(hp1);
	    String service2 = getService(hp2);

	    // service types do not match
	    if (!service1.equalsIgnoreCase(service2)) {
		return false;
	    }

	    String host1 = getHost(hp1);
	    String host2 = getHost(hp2);

	    int i1=0;
	    int i2=0;
	    int s1 = host1.length();
	    int s2 = host2.length();
	    char h1;
	    char h2;
	    while (i1 < s1 && i2 < s2) {
		h1 = Character.toUpperCase(host1.charAt(i1));
		h2 = Character.toUpperCase(host2.charAt(i2));

		if (h1 == h2) {
		    if (h1 == '.') {
			return host1.equalsIgnoreCase(host2);
		    }
		    i1++;
		    i2++;
		} else if (h1 == '.' && h2 == '-') {
		    return compareHost(host2, i2, host1, i1);
		} else if (h1 == '-' && h2 == '.') {
		    return compareHost(host1, i1, host2, i2);
		} else {
		    return false;
		}
	    }
	    return (i1 == i2);

	} else {
	    // perform regular comparison

	    // cross-check getStringNameType()
	    // that's not implemented right now

	    return toString().equalsIgnoreCase(another.toString());
	}
    }

    /**
     * Returns globus ID string representation of the name.
     * If name represents is an anonymous name string
     * "<anonymous>" is returned.
     */
    public String toString() {
	if (this.name == null) {
	    return "<anonymous>";
	} else {
	    if (this.globusID == null) {
		this.globusID = PureTLSUtil.toGlobusID(this.name);
	    }
	    return this.globusID;
	}
    }

    protected String getHostPart(boolean first) {
	Vector dn = this.name.getName();
	int len = dn.size();
	if (first) {
	    for (int i=0;i<len;i++) {
		Vector rdn = (Vector)dn.elementAt(i);
		String [] ava = (String[])rdn.elementAt(0);
		if (ava[0].equalsIgnoreCase("CN")) {
		    return ava[1];
		}
	    }
	} else {
	    for (int i=len-1;i>=0;i--) {
		Vector rdn = (Vector)dn.elementAt(i);
		String [] ava = (String[])rdn.elementAt(0);
		if (ava[0].equalsIgnoreCase("CN")) {
		    return ava[1];
		}
	    }
	}
	return null;
    }

    private static String getService(String name) {
	int pos = name.indexOf('/');
	return (pos == -1) ? "host" : name.substring(0, pos);
    }
    
    private static String getHost(String name) {
	int pos = name.indexOf('/');
	return (pos == -1) ? name : name.substring(pos+1);
    }

    private static boolean compareHost(String host1, int i, 
				       String host2, int j) {
	if (host1.charAt(i) != '-') {
	    throw new IllegalArgumentException();
	}
	int size = host1.length();
	while (i < size ) {
	    if (host1.charAt(i) == '.') {
		break;
	    } else {
		i++;
	    }
	}
	if (size - i == host2.length() - j) {
	    return host1.regionMatches(i, 
				       host2,
				       j, 
				       size - i);
	} else {
	    return false;
	}
    }

    // ----------------------------------

    /**
     * Currently not implemented.
     */
    public Oid getStringNameType()
	throws GSSException {
	throw new GSSException(GSSException.UNAVAILABLE);
    }

    /**
     * Currently not implemented.
     */
    public byte[] export()
	throws GSSException {
	throw new GSSException(GSSException.UNAVAILABLE);
    }

    /**
     * Currently not implemented.
     */
    public GSSName canonicalize(Oid mech)
	throws GSSException {
	throw new GSSException(GSSException.UNAVAILABLE);
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {

        oos.writeObject(this.nameType);
        Vector oids = this.name.getName();
        oos.writeObject(oids);
    }

    private void readObject(ObjectInputStream ois) 
        throws IOException, ClassNotFoundException {

        this.nameType = (Oid)ois.readObject();
        this.name = new X509Name((Vector)ois.readObject());
    }
}
