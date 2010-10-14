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
 * This class represnts a variable definitions in the RSL string 
 * (see rsl_substitution attribute)
 *
 */
public class Bindings extends NameValue {

    public Bindings(String attribute) {
	super(attribute);
    }

    /**
     * Adds a new variable definition to the list.
     *
     * @param binding a new variable definition.
     */
    public void add(Binding binding) {
	if (values == null) values = new LinkedList();
	values.add(binding);
    }

    /**
     * Removes a specific variable definition from
     * the list of bindings. The variable name must
     * match exactly to be removed (it is case sensitive).
     *
     * @param varName variable name to remove the definition of.
     * @return true if the variable was successfully removed. 
     *         False, otherwise.
     */
    public boolean removeVariable(String varName) {
	if (values == null) return false;
        Iterator iter = values.iterator();
        Binding binding;
        int i=0;
        int found = -1;
        while( iter.hasNext() ) {
            binding = (Binding)iter.next();
	    if (binding.getName().equals(varName)) {
		found = i;
		break;
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

    /**
     * Evaluates the variable definitions as variable definitions
     * can reference each other against the symbol table.
     * The evaluation process updates the symbol table.
     * 
     * @param symbolTable the symbol table to evalute the variables
     *        against.
     * @return a new evaluted variable definition.
     * @exception RslEvaluationException If an error occured during 
     *            rsl evaluation.
     */
    public Bindings evaluate(Map symbolTable) 
	throws RslEvaluationException {
	if (symbolTable == null) {
	    throw new IllegalArgumentException("Symbol table must be initialized.");
	}
	List newValues = new LinkedList();
	Iterator iter = values.iterator();
	Object vl;
	Binding binding;
	while (iter.hasNext()) {
	    vl = iter.next();
	    if (vl instanceof Binding) {
		binding = ((Binding)vl).evaluate(symbolTable);
		
		// update symbol table
		symbolTable.put(binding.getName(),
				binding.getValue().getValue());
		
		newValues.add(binding);
	    } else {
		// error: only binding objects should be in the list
		throw new RuntimeException("Invalid object in binding");
	    }
	}
	
	Bindings bind = new Bindings(getAttribute());
	bind.setValues(newValues);
	return bind;
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
	buf.append(" = ");
	Iterator iterator = values.iterator();
	Binding binding;
	while(iterator.hasNext()) {
	    binding = (Binding)iterator.next();
	    binding.toRSL(buf, explicitConcat);
	    if (iterator.hasNext()) buf.append(" ");
	}
	buf.append(" )");
    }
    
}
