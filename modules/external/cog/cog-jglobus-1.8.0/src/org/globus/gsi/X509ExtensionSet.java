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

import java.util.Set;
import java.util.Hashtable;

import org.globus.util.I18n;

/** 
 * Represents a set of X.509 extensions.
 */
public class X509ExtensionSet {

    private static I18n i18n =
        I18n.getI18n("org.globus.gsi.errors",
                     X509ExtensionSet.class.getClassLoader());
   
    private Hashtable extensions;

    /**
     * Creates an empty X509ExtensionSet object.
     */
    public X509ExtensionSet() {
	this.extensions = new Hashtable();
    }

    /**
     * Adds a X509Extension object to this set.
     *
     * @param extension the extension to add
     * @return an extension that was removed with the same oid as the
     *         new extension. Null, if none existed before.
     */
    public X509Extension add(X509Extension extension) {
	if (extension == null) {
	    throw new IllegalArgumentException(i18n
                                               .getMessage("extensionNull"));
	}
	return (X509Extension)this.extensions.put(extension.getOid(),
						  extension);
    }
    
    /**
     * Retrieves X509Extension by given oid.
     *
     * @param oid the oid of the extension to retrieve.
     * @return the extension with the specified oid. Can be null if
     *         there is no extension with such oid.
     */
    public X509Extension get(String oid) {
	if (oid == null) {
	    throw new IllegalArgumentException(i18n.getMessage("oidNull"));
	}
	return (X509Extension)this.extensions.get(oid);
    }

    /**
     * Removes X509Extension by given oid.
     *
     * @param oid the oid of the extension to remove.
     * @return extension that was removed. Null, if extension with the
     *         specified oid does not exist in this set.
     */
    public X509Extension remove(String oid) {
	if (oid == null) {
	    throw new IllegalArgumentException(i18n.getMessage("oidNull"));
	}
	return (X509Extension)this.extensions.remove(oid);
    }
    
    /**
     * Returns the size of the set.
     *
     * @return the size of the set.
     */
    public int size() {
	return this.extensions.size();
    }
    
    /**
     * Returns if the set is empty.
     *
     * @return true if the set if empty, false otherwise.
     */
    public boolean isEmpty() {
	return this.extensions.isEmpty();
    }
    
    /**
     * Removes all extensions from the set.
     */
    public void clear() {
	this.extensions.clear();
    }
    
    /**
     * Returns a set view of the OIDs of the extensions contained in this
     * extension set.
     *
     * @return the set with oids.
     */
    public Set oidSet() {
	return this.extensions.keySet();
    }
    
}
