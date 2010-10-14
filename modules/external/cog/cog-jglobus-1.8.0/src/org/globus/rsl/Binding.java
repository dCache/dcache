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
 * This class represents a single variable definition in RSL 
 * (see rsl_substitution attribute)
 */
public class Binding {

    protected String _name;
    protected Value _value;

    public Binding(String name, Value value) {
	_name = name;
	_value = value;
    }

    public Binding(String name, String value) {
	_name = name;
	_value = new Value(value);
    }
    
    /**
     * Returns the name of the variable.
     *
     * @return the variable name.
     */
    public String getName() {
	return _name;
    }
    
    /**
     * Returns the variable value.
     *
     * @return the variable value.
     */
    public Value getValue() {
	return _value;
    }
    
    /**
     * Evaluates the variable definition with the specified 
     * symbol table.
     *
     * @param symbolTable the symbol table to evaluate
     *        the value against.
     * @return an evaluated string.
     * @exception RslEvaluationException If an error occured during 
     *            rsl evaluation.
     */
    public Binding evaluate(Map symbolTable) 
	throws RslEvaluationException {
	String strValue = _value.evaluate(symbolTable);
	return new Binding(getName(), new Value(strValue));
    }

    /**
     * Returns a RSL representation of this variable definition.
     *
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     * @return RSL representation of this variable definition.
     */
    public String toRSL(boolean explicitConcat) {
	StringBuffer buf = new StringBuffer();
	toRSL(buf, explicitConcat);
	return buf.toString();
    }
    
    /**
     * Produces a RSL representation of this variable definition.
     *
     * @param buf buffer to add the RSL representation to.
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     */
    public void toRSL(StringBuffer buf, boolean explicitConcat) {
	buf.append("(");
	buf.append( getName() );
	buf.append(" ");
        getValue().toRSL(buf, explicitConcat);
	buf.append(")");
    }
    
    public String toString() {
	return toRSL(true);
    }
    
}
