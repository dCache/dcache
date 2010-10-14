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
 * This class represents a single relation in the RSL string.
 */
public class NameOpValue extends NameValue {

    public static final int EQ    = 1;
    public static final int NEQ   = 2;
    public static final int GT    = 3;
    public static final int GTEQ  = 4;
    public static final int LT    = 5;
    public static final int LTEQ  = 6;

    protected int operator;

    public NameOpValue(String attribute) {
	super(attribute);
    }

    public NameOpValue(String attribute, int op) {
	super(attribute);
	setOperator(op);
    }
    
    public NameOpValue(String attribute, int op, String strValue) {
	this(attribute, op);
	add(strValue);
    }
    
    public NameOpValue(String attribute, int op, String [] strValues) {
	this(attribute, op);
	add(strValues);
    }

    public NameOpValue(String attribute, int op, Value value) {
	this(attribute, op);
	add(value);
    }
    
    /**
     * Sets the relation operator.
     *
     * @param oper the relation operator.
     */
    public void setOperator(int oper) {
	operator = oper;
    }

    /**
     * Returns the relation operator.
     *
     * @return the relation operator.
     */
    public int getOperator() {
	return operator;
    }

    /**
     * Returns the relation operator as a string.
     *
     * @return the relation operator as a string.
     */
    public String getOperatorAsString() {
	return getOperatorAsString(operator);
    }

    /**
     * Returns a string representation of the specified 
     * relation operator.
     *
     * @param op the relation operator
     * @return the string representaion of the relation operator.
     */
    public static String getOperatorAsString(int op) {
	switch(op) {
	case EQ: 
	    return "=";
	case NEQ: 
	    return "!=";
	case GT: 
	    return ">";
	case GTEQ: 
	    return ">=";
	case LT: 
	    return "<";
	case LTEQ: 
	    return "<=";
	default: return "??";
	}
    }
    
    /**
     * Adds a value to the list of values.
     *
     * @param value the value to add.
     */
    public void add(Value value) {
	if (values == null) values = new LinkedList();
	values.add(value);
    }
    
    /**
     * Adds a value to the list of values.
     * The string value is first converted into
     * a Value object.
     *
     * @param strValue the value to add.
     */
    public void add(String strValue) {
	add(new Value(strValue));
    }

    /**
     * Adds an array of values to the list of values.
     * Each element in the array is converted into a 
     * Value object and inserted as a separate value
     * into the list of values.
     *
     * @param strValues the array of values to add.
     */
    public void add(String [] strValues) {
	if (strValues == null) return;
	if (values == null) values = new LinkedList();
	for (int i=0;i<strValues.length;i++) {
	    values.add( new Value(strValues[i]) );
	}
    }

    /**
     * Adds a list to the list of values. It is inserted
     * as a single element.
     *
     * @param list the list to add.
     */
    public void add(List list) {
	if (values == null) values = new LinkedList();
	values.add(list);
    }
    
    public List getValuesAsStrings(boolean includeNested) {
	return getValuesAsString( getValues(), includeNested );
    }
    
    public static List getValuesAsString(List values, 
					 boolean includeNested) {
	if (values == null) return null;
	List list = new LinkedList();
	Iterator iter = values.iterator();
	Object obj;
	while(iter.hasNext()) {
	    obj = iter.next();
	    if (obj instanceof Value) {
		list.add( ((Value)obj).getCompleteValue() );
	    } else if (includeNested && obj instanceof List) {
		list.add( getValuesAsString( (List)obj, includeNested ) );
	    } 
	}
	return list;
    }

    /**
     * Produces a RSL representation of this relation.
     *
     * @param buf buffer to add the RSL representation to.
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     */    
    public void toRSL(StringBuffer buf, boolean explicitConcat) {
	buf.append("( ");
	buf.append( getAttribute() );
	buf.append(" ");
	buf.append(getOperatorAsString(operator));
	buf.append(" ");
	toRSLSub(getValues(), buf, explicitConcat);
	buf.append(" )");
    }

    private void toRSLSub(List values, StringBuffer buf, 
			  boolean explicitConcat) {
	Iterator iter = values.iterator();
	Object obj;
	while(iter.hasNext()) {
	    obj = iter.next();
	    if (obj instanceof Value) {
		((Value)obj).toRSL(buf, explicitConcat);
	    } else if (obj instanceof List) {
		buf.append("( ");
		toRSLSub( (List)obj, buf, explicitConcat );
		buf.append(" )");
	    } else {
		// error: only Values and Lists are allowed.
		throw new RuntimeException("Invalid object in relation values");
	    }
	    if (iter.hasNext()) buf.append(" ");
	}
    }

    /**
     * Evaluates the relation against the symbol table.
     * 
     * @param symbolTable the symbol table to evalute the relation
     *        against.
     * @return a new evaluted relation.
     * @exception RslEvaluationException If an error occured during 
     *            rsl evaluation.
     */
    public NameOpValue evaluate(Map symbolTable) 
	throws RslEvaluationException {
	
	List list = evaluateSub(values, symbolTable);
	
	NameOpValue newNV = new NameOpValue(getAttribute(),
					    getOperator());
	
	newNV.setValues(list);
	
	return newNV;
    }
    
    private List evaluateSub(List values, Map symbolTable) 
	throws RslEvaluationException {
	List newValues = new LinkedList();
	Iterator iter = values.iterator();
	Object obj;
	while(iter.hasNext()) {
	    obj = iter.next();
	    if (obj instanceof Value) {
		String str = ((Value)obj).evaluate(symbolTable);
		newValues.add( new Value(str) );
	    } else if (obj instanceof List) {
		newValues.add( evaluateSub( (List)obj, symbolTable ));
	    } else {
		// error: only Values and Lists are allowed.
		throw new RslEvaluationException("Invalid object in relation values");
	    }
	}
	return newValues;
    }

}
