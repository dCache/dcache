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
 * This class represents a simple value (a string) that can be 
 * concatinated with another value.
 */
public class Value {

    protected String value;
    protected Value concatValue;

    public Value(String value) {
	this(value, null);
    }

    public Value(String value, Value concatValue) {
	this.value = value;
	this.concatValue = concatValue;
    }

    public boolean equals(Object obj) {
	if (obj instanceof Value) {
	    Value src = (Value)obj;
	    if (src.getValue() == null) {
		if (getValue() != null) return false;
	    } else {
		if (!src.getValue().equals(getValue())) return false;
	    }
	    if (src.getConcat() == null) {
		if (getConcat() != null) return false;
	    } else {
		if (!src.getConcat().equals(getConcat())) return false;
	    }
	    return true;
	} else {
	    return super.equals(obj);
	}
    }

    public int hashCode() {
        int hashCode = 0;
        if (this.value != null) {
            hashCode += this.value.hashCode();
        }
        if (this.concatValue != null) {
            hashCode += this.concatValue.hashCode();
        }
        return hashCode;
    }

    /**
     * Sets the actual value.
     *
     * @param value the new value.
     */
    public void setValue(String value) {
	this.value = value;
    }

    /**
     * Returns the actual string value.
     *
     * @return the current value.
     */
    public String getValue() {
	return value;
    }
    
    /**
     * Returns the value that is concatinated
     * with this value.
     * 
     * @return the value that is concatinated
     *         with this value. Null, otherwise.
     */
    public Value getConcat() {
	return concatValue;
    }

    /**
     * Appends the specified value to the end of the chain 
     * of concatinated values. That is, if this value has 
     * no concatinated value then set the specified value 
     * as the concatinated value. If this value already
     * has a concatinated value then append the
     * specified value to that concatinated value.
     * 
     * @param value the value to concatinate.
     */
    public void concat(Value value) {
	if (concatValue != null) {
	    concatValue.concat(value);
	} else {
	    concatValue = value;
	}
    }

    /**
     * Evaluates the value with the specified 
     * symbol table.
     * In this case the function just returns the 
     * string representation of the actual value.
     * No symbol table lookups are performed.
     *
     * @param symbolTable the symbol table to evaluate
     *        the value against.
     * @return an evaluated string.
     * @exception RslEvaluationException If an error occured during 
     *            rsl evaluation.
     */
    public String evaluate(Map symbolTable) 
	throws RslEvaluationException {
	if (concatValue == null) {
	    return value;
	} else {
	    StringBuffer buf = new StringBuffer(value);
	    buf.append(concatValue.evaluate(symbolTable));
	    return buf.toString();
	}
    }

    /**
     * Returns a RSL representation of this value.
     *
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     * @return RSL representation of this value.
     */
    public String toRSL(boolean explicitConcat) {
	StringBuffer buf = new StringBuffer();
	toRSL(buf, explicitConcat);
	return buf.toString();
    }

    private String quotify(String str) {
        char curChar;
        char quoteChar = '"';
        int size = str.length();
        StringBuffer buf = new StringBuffer(size+2);
	
        buf.append(quoteChar);
	
        for (int i=0;i<size;i++) {
            curChar = str.charAt(i);
            if (curChar == quoteChar) {
                buf.append(curChar);
            }
            buf.append(curChar);
        }
        buf.append(quoteChar);
        return buf.toString();
    }

    /**
     * Produces a RSL representation of this value.
     *
     * @param buf buffer to add the RSL representation to.
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     */
    public void toRSL(StringBuffer buf, boolean explicitConcat) {

	if ( explicitConcat ) {
            buf.append( quotify( value ) );
        } else {
            buf.append( value );
        }

	if (concatValue == null) {
	    return;
	}

	if (explicitConcat) {
	    buf.append(" # ");
	}
	
	concatValue.toRSL(buf, explicitConcat);
    }

    /**
     * Returns a complete string representation of this
     * value. 
     *
     * @return a complete string representation of this
     *         value.
     */
    public String getCompleteValue() {
	if (concatValue == null) {
	    return value;
	} else {
	    StringBuffer buf = new StringBuffer(value);
	    buf.append(concatValue.getCompleteValue());
	    return buf.toString();
	}
    }
    
    public String toString() {
	return getCompleteValue();
    }
    
}
