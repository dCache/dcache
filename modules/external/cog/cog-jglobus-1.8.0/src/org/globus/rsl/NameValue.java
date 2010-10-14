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
package org.globus.rsl;

import java.util.*;

/**
 * This class represents an attribute that is associated with multiple values.
 */
public abstract class NameValue {

    protected String attribute;
    protected List values; 

    public NameValue(String attribute) {
	setAttribute(attribute);
    }

    /**
     * Sets the attribute name.
     *
     * @param attrib the attribute name.
     */
    public void setAttribute(String attrib) {
	attribute = attrib;
    }

    /**
     * Sets the list of values.
     *
     * @param values list of values.
     */
    public void setValues(List values) {
	this.values = values;
    }

    /**
     * Returns the attribute name.
     *
     * @return the attribute name.
     */
    public String getAttribute() {
	return attribute;
    }

    /**
     * Returns the list of values.
     *
     * @return list of values.
     */
    public List getValues() {
	return values;
    }

    /**
     * Removes all values associated
     * with the attribute.
     */
    public void clear() {
	if (values != null) values.clear();
    }

    /**
     * Removes a specific value from the list of values.
     * The value must be matched exactly to be removed
     * from the list.
     *
     * @param value value to remove.
     * @return true if the value was successfuly removed
     *         from the list of values. False, otherwise.
     */
    public boolean remove(Object value) {
        if (values == null) return false;
        return values.remove(value);
    }
    
    /**
     * Returns a RSL representation of this relation.
     *
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     * @return RSL representation of this relation.
     */
    public String toRSL(boolean explicitConcat) {
	StringBuffer buf = new StringBuffer();
	toRSL(buf, explicitConcat);
	return buf.toString();
    }
    
    /**
     * Produces a RSL representation of this relation.
     *
     * @param buf buffer to add the RSL representation to.
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     */
    public abstract void toRSL(StringBuffer buf, boolean explicitConcat);

    /**
     * Returns the first value (if any) in the
     * list.
     *
     * @return the first value. Null, if there
     *         is no values at all.
     */
    public Object getFirstValue() {
	if (values == null || values.size() == 0) return null;
	return values.get(0);
    }

    public boolean merge(NameValue nv) {
	if (!getAttribute().equalsIgnoreCase(nv.getAttribute())) return false;
	List nvValues = nv.getValues();
	if (nvValues == null) return false;
	
	if (values == null) values = new LinkedList();
	
	Iterator iter = nvValues.iterator();
	while( iter.hasNext() ) {
	    values.add( iter.next() );
	}
	
	return true;
    }
    
    public String toString() {
	return toRSL(true);
    }
    
}
