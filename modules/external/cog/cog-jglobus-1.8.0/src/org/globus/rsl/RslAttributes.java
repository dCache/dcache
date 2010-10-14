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
 * This class provides convieniene methods for
 * accessing and manipulatig simple rsl expressions.
 * The class provides methods for retreiving and setting
 * values of specified attributes.
 */
public class RslAttributes {
    
    protected RslNode rslTree;
    
    /**
     * Creates an empty RslAttributes object.
     */
    public RslAttributes() {
	rslTree = new RslNode();
    }

    /**
     * Creates a new RslAttributes object with
     * specified rsl parse tree.
     *
     * @param rslTree the rsl parse tree.
     */
    public RslAttributes(RslNode rslTree) {
	this.rslTree = rslTree;
    }

    /**
     * Creates a new RslAttributes object from
     * specified RSL string. 
     *
     * @param rsl the rsl string.
     * @exception ParseException if the rsl cannot be parsed.
     */
    public RslAttributes(String rsl) 
	throws ParseException {
	rslTree = RSLParser.parse(rsl);
    }

    /**
     * Returns the rsl parse tree.
     *
     * @return the rsl parse tree.
     */
    public RslNode getRslNode() {
	return rslTree;
    }

    /**
     * Returns a string value of the specified attribute.
     * If the attribute contains multiple values the
     * first one is returned.
     *
     * @param attribute the rsl attribute to return the value of.
     * @return value of the relation. Null is returned if there is
     *         no such attribute of the attribute/value relation is
     *         not an equality relation.
     */
    public String getSingle(String attribute) {
	NameOpValue nv = rslTree.getParam(attribute);
	if (nv == null || nv.getOperator() != NameOpValue.EQ) return null;
	Object obj = nv.getFirstValue();
	if (obj != null && obj instanceof Value) {
	    return ((Value)obj).getCompleteValue();
	} else {
	    return null;
	}	
    }
    
    /**
     * Returns a list of strings for a specified attribute.
     * For example for 'arguments' attribute.
     *
     * @param attribute the rsl attribute to return the values of.
     * @return the list of values of the relation. Each value is 
     *         a string. Null is returned if there is no such
     *         attribute or the attribute/values relation is not
     *         an equality relation.
     */
    public List getMulti(String attribute) {
	NameOpValue nv = rslTree.getParam(attribute);
	if (nv == null || nv.getOperator() != NameOpValue.EQ) return null;
	List values = nv.getValues();
	List list = new LinkedList();
	Iterator iter = values.iterator();
	Object obj;
	while( iter.hasNext() ) {
	    obj = iter.next();
	    if (obj instanceof Value) {
		list.add( ((Value)obj).getCompleteValue() );
	    }
	}
	return list;
    }

    /**
     * Returns a key/value pair map for a specified attribute.
     * For example for 'environment' attribute.
     * Note: <I>Use getVariables() for rsl_substitution attribute.</I>
     *
     * @param attribute the rsl attribute to return the key/value pair map of.
     * @return a key/value pair map. Null is returned if there is no such
     *         attribute defined or if the attribute/value relation is not
     *         an equality relation.
     */
    public Map getMap(String attribute) {
	NameOpValue nv = rslTree.getParam(attribute);
	if (nv == null || nv.getOperator() != NameOpValue.EQ) return null;
	List values = nv.getValues();
	Map map = new HashMap();
	Iterator iter = values.iterator();
	Object obj;
	while( iter.hasNext() ) {
	    obj = iter.next();
	    if (obj instanceof List) {
		String key, value;
		List list = (List)obj;
		if (list.size() != 2) continue; // must have 2 values!
		obj = list.get(0);
		if (obj instanceof Value) {
		    key = ((Value)obj).getCompleteValue();
		} else {
		    continue;
		}
		obj = list.get(1);
		if (obj instanceof Value) {
		    value = ((Value)obj).getCompleteValue();
		} else {
		    continue;
		}
		map.put(key, value);
	    }
	}
	return map;
    }
    
    /**
     * Returns a variable name/value pair map of variable definitions.
     * Currently specified by the 'rsl_substitution' attribute.
     *
     * @param attribute the attribute that defines variables. Currently,
     *        only 'rsl_substitution' is supported.
     * @return a variable name/value pair map. Null, if there is no
     *          definitions for a specified attribute.
     *
     */    
    public Map getVariables(String attribute) {
	Bindings binds = rslTree.getBindings(attribute);
	if (binds == null) return null;
        List values = binds.getValues();
        Map map = new HashMap();
        Iterator iter = values.iterator();
	Binding binding;
        while( iter.hasNext() ) {
            binding = (Binding)iter.next();
	    map.put(binding.getName(),
		    binding.getValue().getCompleteValue());
        }
        return map;
    }
    
    /**
     * Adds a new variable definition to the specified variable definitions
     * attribute.
     *
     * @param attribute the variable definitions attribute - rsl_subsititution.
     * @param varName the variable name to add.
     * @param value the value of the variable to add.
     */
    public void addVariable(String attribute, String varName, String value) {
	Bindings binds = rslTree.getBindings(attribute);
	if (binds == null) {
	    binds = new Bindings(attribute);
	    rslTree.put(binds);
	}
	binds.add(new Binding(varName, value));
    }
    
    /**
     * Removes a specific variable definition given a variable name.
     *
     * @param attribute the attribute that defines variable definitions.
     * @param varName the name of the variable to remove.
     * @return true if the variable was successfully removed. Otherwise,
     *         returns false, 
     */
    public boolean removeVariable(String attribute, String varName) {
	Bindings binds = rslTree.getBindings(attribute);
        if (binds == null) return false;
	return binds.removeVariable(varName);
    }

    /**
     * Removes a specific attribute from attribute/value relations.
     *
     * @param attribute the attribute name to remove.
     */
    public void remove(String attribute) {
	rslTree.removeParam(attribute);
    }

    /**
     * Removes a specific value from a list of values of the specified
     * attribute.
     *
     * @param attribute the attribute from which to remote the value from.
     * @param value the specific value to remove.
     * @return true if the value was successfully removed. Otherwise,
     *         returns false, 
     */
    public boolean remove(String attribute, String value) {
	NameOpValue nv = rslTree.getParam(attribute);
	if (nv == null || nv.getOperator() != NameOpValue.EQ) return false;
	return nv.remove(new Value(value));
    }
    
    /**
     * Removes a specific key from a list of values of the specified
     * attribute. The attribute values must be in the right form. See
     * the 'environment' rsl attribute.
     *
     * @param attribute the attribute to remove the key from.
     * @param key the key to remove.
     * @return true if the key was successfully removed. Otherwise, 
     *         returns false.
     */
    public boolean removeMap(String attribute, String key) {
	NameOpValue nv = rslTree.getParam(attribute);
	if (nv == null || nv.getOperator() != NameOpValue.EQ) return false;
	List values = nv.getValues();
	Iterator iter = values.iterator();
	Object obj;
	int i=0;
	int found = -1;
	while( iter.hasNext() ) {
	    obj = iter.next();
	    if (obj instanceof List) {
		List vr = (List)obj;
		if (vr.size() > 0) {
		    Object var = vr.get(0);
		    if (var instanceof Value &&
			((Value)var).getValue().equals(key)) {
			found = i;
			break;
		    }
		}
	    }
	    i++;
	}
	if (found != -1) {
	    values.remove(found);
	    return true;
	} else {
	    return false;
	}
    }

    protected NameOpValue getRelation(String attribute) {
	NameOpValue nv = rslTree.getParam(attribute);
	if (nv == null) {
	    nv = new NameOpValue(attribute, NameOpValue.EQ);
	    rslTree.put(nv);
	}
	return nv;
    }

    /**
     * Sets the attribute value to the given value.
     * All previous values are removed first.
     *
     * @param attribute the attribute to set the value of.
     * @param value the value to add.
     */
    public void set(String attribute, String value) {
	NameOpValue nv = getRelation(attribute);
	nv.clear();
	nv.add(new Value(value));
    }

    /**
     * Adds a simple value to the list of values of a given
     * attribute.
     *
     * @param attribute the attribute to add the value to.
     * @param value the value to add.
     */
    public void add(String attribute, String value) {
	NameOpValue nv = getRelation(attribute);
	nv.add(new Value(value));
    }    
    
    /**
     * Sets the attribute value to the given list of values.
     * The list of values is added as a single value.
     *
     * @param attribute the attribute to set the value of.
     * @param values the list of values to add.
     */
    public void setMulti(String attribute, String [] values) {
	NameOpValue nv = getRelation(attribute);
	nv.clear();
	List list = new LinkedList();
	for (int i=0;i<values.length;i++) {
	    list.add(new Value(values[i]));
	}
	nv.add(list);
    }

    /**
     * Adds a list of values as a single value to the specified
     * attribute.
     *
     * @param attribute the attribute to add the list of values to.
     * @param values the values to add.
     */
    public void addMulti(String attribute, String [] values) {
	NameOpValue nv = getRelation(attribute);
	
	List list = new LinkedList();
	for (int i=0;i<values.length;i++) {
	    list.add(new Value(values[i]));
	}
	nv.add(list);
    }    
    
    // --- Compatibility API with old RslAttributes class ---
    
    /**
     * Returns the first value of a specified attribute.
     *
     * @return the first value of the attribute.
     * @deprecated use getSingle() instead.
     */
    public String getFirstValue(String attribute) {
	return getSingle(attribute);
    }

    /**
     * Returns the values for a specified attribute.
     *
     * @return the list of values.
     * @deprecated use getMulti() instead.
     */
    public List get(String attribute) {
        return getMulti(attribute);
    }

    /**
     * Returns the rsl.
     *
     * @return the rsl.
     */
    public String toRSL() {
	return rslTree.toRSL(true);
    }
    
}
