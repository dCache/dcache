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

import org.bouncycastle.asn1.DEREncodable;

import org.globus.gsi.X509Extension;

import org.globus.util.I18n;

/** 
 * A convenience class for creating X.509 extensions from 
 * <code>DEREncodable</code> objects.
 */
public class BouncyCastleX509Extension extends X509Extension {

    private static I18n i18n =
        I18n.getI18n("org.globus.gsi.errors",
                     BouncyCastleX509Extension.class.getClassLoader());

    public BouncyCastleX509Extension(String oid) {
	this(oid, false, null);
    }
    
    public BouncyCastleX509Extension(String oid, DEREncodable value) {
	this(oid, false, value);
    }
    
    public BouncyCastleX509Extension(String oid, boolean critical, 
				     DEREncodable value) {
	super(oid, critical, null);
	setValue(value);
    }
    
    protected void setValue(DEREncodable value) {
	if (value == null) {
	    return;
	}
    	try {
	    setValue(BouncyCastleUtil.toByteArray(value.getDERObject()));
	} catch (IOException e) {
	    throw new RuntimeException(i18n.getMessage("byteArrayErr") +
				       e.getMessage());
	}
    }
}
