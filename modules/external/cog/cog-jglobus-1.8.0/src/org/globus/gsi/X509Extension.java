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

import org.globus.util.I18n;

import java.io.IOException;
import java.security.cert.X509Certificate;

import org.globus.gsi.bc.BouncyCastleUtil;

/** 
 * Represents an X.509 extension. It is used to create X.509 extensions
 * and pass them in a <code>X509ExtensionSet</code> during certificate
 * generation.
 */
public class X509Extension {
   
    protected boolean critical;
    protected byte[] value;
    protected String oid;

    private static I18n i18n =
        I18n.getI18n("org.globus.gsi.errors",
                     X509Extension.class.getClassLoader());

    /**
     * Creates a X509Extension object with specified oid.
     * The extension has no value and is marked as noncritical.
     *
     * @param oid the oid of the extension
     */
    public X509Extension(String oid) {
	this(oid, false, null);
    }

    /**
     * Creates a X509Extension object with specified oid and value.
     * The extension is marked as noncritical.
     *
     * @param oid the oid of the extension
     * @param value the actual value of the extension (not octet string 
     *        encoded). The value can be null.
     */
    public X509Extension(String oid, byte[] value) {
	this(oid, false, value);
    }

    /**
     * Creates a X509Extension object with specified oid, critical property,
     * and value.
     *
     * @param oid the oid of the extension
     * @param critical the critical value.
     * @param value the actual value of the extension (not octet string 
     *        encoded). The value can be null.
     */
    public X509Extension(String oid, boolean critical, byte[] value) {
	if (oid == null) {
	    throw new IllegalArgumentException(i18n.getMessage("oidNull"));
	}
	this.oid = oid;
	this.critical = critical;
	this.value = value;
    }

    /**
     * Sets the oid of this extension.
     *
     * @param oid the oid of this extension. Cannot not null.
     */
    public void setOid(String oid) {
	if (oid == null) {
	    throw new IllegalArgumentException(i18n.getMessage("oidNull"));
	}
	this.oid = oid;
    }

    /**
     * Returns the oid of this extension.
     *
     * @return the oid of this extension. Always non-null.
     */
    public String getOid() {
	return this.oid;
    }

    /**
     * Sets the extension as critical or noncritical.
     *
     * @param critical the critical value.
     */
    public void setCritical(boolean critical) {
	this.critical = critical;
    }

    /**
     * Determines whether or not this extension is critical.
     *
     * @return true if extension is critical, false otherwise.
     */
    public boolean isCritical() {
	return this.critical;
    }

    /**
     * Sets the actual value of the extension (not octet string encoded).
     *
     * @param value the actual value of the extension. Can be null.
     */
    public void setValue(byte [] value) {
	this.value = value;
    }

    /**
     * Returns the actual value of the extension (not octet string encoded)
     *
     * @return the actual value of the extension (not octet string encoded).
     *         Null if value not set.
     */
    public byte[] getValue() {
	return this.value;
    }
    
    /**
     * Returns the actual value of the extension.
     *
     * @param cert the certificate that contains the extensions to retrieve.
     * @param oid the oid of the extension to retrieve.
     * @return the actual value of the extension (not octet string encoded)
     * @exception IOException if decoding the extension fails.
     */
    public static byte[] getExtensionValue(X509Certificate cert, String oid) 
	throws IOException {
	if (cert == null) {
	    throw new IllegalArgumentException(i18n.getMessage("certNull"));
	}
	if (oid == null) {
	    throw new IllegalArgumentException(i18n.getMessage("oidNull"));
	}
	
	byte [] value = cert.getExtensionValue(oid);
	if (value == null) {
	    return null;
	}
	
	return BouncyCastleUtil.getExtensionValue(value);
    }
}
