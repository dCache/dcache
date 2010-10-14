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
 * This class represents a variable reference in the RSL string. 
 * The reference can be concatinated by other values.
 */
public class VarRef extends Value {

    protected Value defValue;

    public VarRef(String varReference) {
	this(varReference, null, null);
    }
    
    public VarRef(String varReference, Value defValue) {
	this(varReference, defValue, null);
    }
    
    public VarRef(String varReference, Value defValue, Value concatVal) {
	super(varReference, concatVal);
	setDefaultValue(defValue);
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof VarRef && super.equals(obj)) {
            VarRef other = (VarRef)obj;
            if (this.defValue == null) {
                return (other.defValue == null);
            } else if (other.defValue == null) {
                return false;
            } else {
                return (this.defValue.equals(other.defValue));
            }
        }
        return false;
    }

    public int hashCode() {
        int hashCode = super.hashCode();
        if (this.defValue != null) {
            hashCode += this.defValue.hashCode();
        }
        return hashCode;
    }

    /**
     * Sets the default value of this reference.
     *
     * @param value the default value.
     */
    public void setDefaultValue(Value value) {
	defValue = value;
    }

    /**
     * Evaluates the variable reference with the specified 
     * symbol table.
     * The value of the reference is first looked up in the 
     * symbol table. If not found, then the default value
     * is used. If the default value is not specified,
     * the reference is evaluated to an empty string.
     *
     * @param symbolTable the symbol table to evaluate
     *        the variabled reference against.
     * @return an evaluated string.
     * @exception RslEvaluationException If an error occured during 
     *            rsl evaluation.
     */
    public String evaluate(Map symbolTable) 
	throws RslEvaluationException {
	String var = null;
	
	if (symbolTable != null) {
	    var = (String)symbolTable.get(value);
	}
	
	if (var == null && defValue != null) {
	    var = defValue.evaluate(symbolTable);
	}
	
	if (var == null) {
	    /* NOTE: according to the rsl specs the variables
	     * should be replaces with empty string.
	     * however, in real code an error is returned.
	     */
	    throw new RslEvaluationException("Variable '" + value + "' not defined.");
	}
	
	if (concatValue == null) {
	    return var;
	} else {
	    return var + concatValue.evaluate(symbolTable);
	}
    }

    /**
     * Produces a RSL representation of this variable reference.
     *
     * @param buf buffer to add the RSL representation to.
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     */    
    public void toRSL(StringBuffer buf, boolean explicitConcat) {
	buf.append("$(");
	buf.append( value );
	
	if (defValue != null) {
	    buf.append(" ");
	    defValue.toRSL(buf, explicitConcat);
	}
	
	buf.append(")");
	
	if (concatValue == null) return;
	
	if (explicitConcat) buf.append(" # ");
	
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
	StringBuffer buf = new StringBuffer();
	buf.append("$(");
	buf.append(value);
	buf.append(")");
	if (concatValue != null) {
	    buf.append(concatValue.getCompleteValue());
	}
	return buf.toString();
    }
    
    public String toString() {
	return getCompleteValue();
    }
    
}
