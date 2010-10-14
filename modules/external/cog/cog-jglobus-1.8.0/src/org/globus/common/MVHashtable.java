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
package org.globus.common;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Enumeration;
import java.io.Serializable;

/** 
 * MVHashtable is an implementation of a multi-valued hashtable.
 * Each entry of the hashtable is identified by a String key.  Besides
 * the key being a String rather than an object, MVHashtable
 * differs from Hashtable in that it maps a single key to multiple
 * values. 
 * Each attribute uses a vector for storing multiple values
 * under the same key.
 * Methods are available which help access and manipulate the entries.
 * Where possible, methods are named analogous to similar methods in Hashtable.
 */
public class MVHashtable implements Serializable {

    /** holds the attributes of the MVHashtable   */
    protected Hashtable attribs;

    /** 
     * Default Constructor creates empty MVHashtable.
     */
    public MVHashtable() {
	attribs = new Hashtable();
    }

    /**
     * Initial-size Constructor creates empty MVHashtable with specified
     * initial size.
     * 
     * @param initialSize the initial capacity of the hashtable.
     */
    public MVHashtable( int initialSize ) {
	attribs = new Hashtable( initialSize );
    }

    /**
     * Copy Constructor
     * 
     * @param multivaluedHashtable the MVHashtable that will start as the base of
     *        this new MVHashtable
     */
    public MVHashtable( MVHashtable multivaluedHashtable ) {
	attribs = new Hashtable();
	Enumeration e = multivaluedHashtable.keys();
	while(e.hasMoreElements()) {
	    String key = (String)e.nextElement();
	    Vector values = (Vector)multivaluedHashtable.attribs.get(key);
	    add(key,values);
	}
    }
    
    /**********************************************************************/
    /** SETTING VALUES */ 
    /**********************************************************************/

    /** 
     * Sets a new value for the specified attribute.
     *
     * @param key the attribute key.
     * @param value the entry value.
     * @return <code>Vector</code> of previous values associated with 
     *         the attribute. Null, if the attribute was not associated
     *         with any values.
     */
    public Vector set(String key, Object value) {
	Vector values = (Vector)attribs.get(key);
	
	Vector newValues = new Vector(1);
	newValues.addElement(value);
	
	attribs.put(key, newValues);
	
	return values;
    }

    /**********************************************************************/
    /** ADDING VALUES */ 
    /**********************************************************************/
    
    /**
     * Adds a single value to the specified key. 
     * If there is already an attribute with this key, the
     * value is appended to the end of values list. 
     *
     * @param key the attribute key.
     * @param value the value to add.
     */
    public void add(String key, Object value) {
	Vector values = (Vector)attribs.get(key);
	if (values == null) {
	    values = new Vector(1);
	    attribs.put(key, values);
	}
	values.addElement(value);
    }
    
    /** 
     * Adds multiple values to the attribute associated
     * with the given key. The values are appended
     * to the end of the values list (if any).
     * 
     * @param key the attribute key.
     * @param values the array of values.
     */
    public void add(String key, Object [] values) {
	Vector ivalues = (Vector)attribs.get(key);
	if (ivalues == null) {
	    ivalues = new Vector(values.length);
	    attribs.put(key, ivalues);
	}
	int size = values.length;
	for (int i=0;i<size;i++) {
	    ivalues.add( values[i] );
	}
    }
    
    /** 
     * Adds multiple values to the attribute associated 
     * with the given key. The values are appended
     * to the end of the values list (if any).
     * 
     * @param key the attribute key.
     * @param values the Vector of values to add.
     */
    public void add(String key, Vector values) {
	Vector ivalues = (Vector)attribs.get(key);
	if (ivalues == null) {
	    ivalues = new Vector(values.size());
	    attribs.put(key, ivalues);
	}
	int size = values.size();
	for (int i=0;i<size;i++) {
	    ivalues.add( values.elementAt(i) );
	}
    }

    /**
     * Adds attributes that are stored in a seperate MVHashtable object.
     * 
     * @param multivaluedHashtable The to-be-absorbed MVHashtable
     */
    public void add(MVHashtable multivaluedHashtable) {
	Enumeration e = multivaluedHashtable.keys();
	while(e.hasMoreElements()) {
	    String key = (String)e.nextElement();
	    Vector values = multivaluedHashtable.get(key);
	    add(key,values);
	}
    }
    
    /**********************************************************************/
    /** REMOVING VALUES */ 
    /**********************************************************************/
    
    /** 
     * Remove an attribute and all values associated with it.
     * 
     * @param key the attribute key.
     * @return the value to which the key had been mapped in this multivalued
     *         hashtable, or null if the key did not have a mapping. 
     */
    public Object remove(String key) {
	return attribs.remove(key);
    }
    
    /** 
     * Removes a particular value within an attribute.
     * 
     * @param key the attribute key.
     * @param value the value to delete.
     * @return true if the value was removed, false otherwise.
     */
    public boolean remove (String key, Object value) {
	Vector values = (Vector)attribs.get(key);
	if (values == null) return false;
	return values.remove(value);
    }

    /**
     * Removes a value at the specified index within the
     * attribute. Note that the indicies of all objects greater
     * than "index" are decremented by one.
     * 
     * @param key the attribute key.
     * @param index the index of the value to delete.
     * @return the element that was removed from the list of valus
     */
    public Object remove(String key, int index) {
	Vector values = (Vector)attribs.get(key);
	if (values == null) return null;
	return values.remove(index);
    }

    /** 
     * Deletes all attributes.
     */
    public void clear() {
	attribs.clear();
    }

    /**********************************************************************/
    /** ENQUIER METHODS */ 
    /**********************************************************************/

    /** 
     * Returns the number of attributes.
     *
     * @return <code>int</code> the number of attributes
     */
    public int size() {
	return attribs.size();
    }

    /**
     * Returns the number of entries for an attribute.
     * 
     * @param key The attribute key.
     *
     * @return <code>int</code> the number of entries of the attribute
     */
    public int size(String key) {
	Vector values = (Vector)attribs.get(key);
	if (values == null) return 0;
	return values.size();
    }

    /**
     * Returns true if the attribute associated with the given key exists.
     * 
     * @param key The attribute key.
     *
     * @return <code>boolean</code> true if attribute exists, false if not
     */
    public boolean containsName(String key) {
	return attribs.containsKey(key);
    }

    /**
     * Returns true if the given value is stored under a specific attribute.
     * 
     * @param key the attribute key.
     * @param value the entry value.
     *
     * @return <code>boolean</code> true if value is stored under specified attribute
     */
    public boolean contains(String key, Object value) {
	Vector values = (Vector)attribs.get(key);
	if (values == null) return false;
	return values.contains(value);
    }

    /** 
     * Returns an enumeration of all attribute keys.
     *
     * @return <code>Enumeration</code> of all attribute keys
     */
    public Enumeration keys() {    
	return attribs.keys();
    }
    
    /**
     * Returns a vector of the keys. Keys are strings.
     *
     * @return <code>Vector</code> of keys
     */
    public Vector getKeys() {
	Vector values = new Vector(attribs.size());
	Enumeration theseVals = attribs.keys();
	while(theseVals.hasMoreElements()) {
	    values.add( (String)theseVals.nextElement() );
	}
	return values;
    }


    /**********************************************************************/
    /** ENQUIER GET METHODS */ 
    /**********************************************************************/

    /**
     * Returns a vector of values for a specific attribute.
     * 
     * @param key the attribute key.
     *
     * @return <code>Vector</code> of values for the specified attribute
     */
    public Vector get(String key) {
	Vector values = (Vector)attribs.get(key);
	if (values == null) return null;
	return (Vector)values.clone();
    }

    /** 
     * Returns the first value for a specific attribute.
     * 
     * @param key the attribute key.
     *
     * @return <code>Object</code> the first value for the specified attribute
     */
    public Object getFirstValue(String key) {
	Vector values = (Vector)attribs.get(key);
	if (values == null) return null;
	return values.firstElement();
    }

    /**
     * Returns the last value for a specific attribute.
     * 
     * @param key the attribute key.
     *
     * @return <code>Object</code> the last value for the specified attribute
     */
    public Object getLastValue(String key) {
	Vector values = (Vector)attribs.get(key);
        if (values == null) return null;
        return values.lastElement();
    }

    /**
     * Returns a value at the given position index for a specific
     * attribute.
     * 
     * @param key the attribute key.
     * @param index the index of the value to return for the attribute.
     *
     * @return <code>Object</code> the value at the specified index position
     * of the specified attribute. Null if not found.
     */
    public Object getValueAt(String key, int index) {
	Vector values = (Vector)attribs.get(key);
        if (values == null) return null;
	if (values.size() > index) {
	    return values.elementAt(index);
	} else {
	    return null;
	}
    }

    /**********************************************************************/
    /** DEBUG METHODS */ 
    /**********************************************************************/
    
    /**
     * Prints out the contents of the data structure. This method is
     * used for debugging purposes.
     */
    public void print() { 
	System.out.println( this.toString() );
    }

    /**
     * Converts the internal data structure to a string.
     *
     * @return <code>String</code> a string representation of this structure.
     */
    public String toString() {
	StringBuffer buf = new StringBuffer();
	String attribute;
	Vector values;
	Enumeration e = keys();
	while(e.hasMoreElements()) {
	    attribute = (String)e.nextElement();
	    values    = get(attribute);

	    buf.append(attribute);
	    buf.append("=");
	    buf.append(values);

	    if (e.hasMoreElements()) buf.append(" ; ");
	}
	return buf.toString();
    }

}
