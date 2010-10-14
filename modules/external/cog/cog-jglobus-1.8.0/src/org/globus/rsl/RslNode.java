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
 * This class represents a RSL parse tree. It is composed of variable definitions 
 * (bindings), relations, and sub-specifications (sub nodes).
 */
/*
 * - when toRSL() explicit concatination is off, the quotes around Values
 *   might be wrong - fix this....
 */
public class RslNode extends AbstractRslNode {
    
    protected Map _relations;
    protected Map _bindings;

    public RslNode() {
	super();
    }
    
    public RslNode(int operator) {
	super(operator);
    }

    public Bindings put(Bindings bindings) {
	if (_bindings == null) _bindings = new HashMap();
	String attrName = bindings.getAttribute();
	return (Bindings)_bindings.put(canonicalize(attrName),
				       bindings);
    }

    public boolean add(Bindings bindings) {
	return (put(bindings) == null);
    }
    
    public NameOpValue put(NameOpValue relation) {
	if (_relations == null) _relations = new HashMap();
	String attrName = relation.getAttribute();
	return (NameOpValue)_relations.put(canonicalize(attrName),
					   relation);
    }
    
    public boolean add(NameOpValue relation) {
	return (put(relation) == null);
    }
    
    public void mergeTo(AbstractRslNode dstNode) {
	Iterator iter = null;
	String attr = null;

	super.mergeTo(dstNode);

	if (_relations != null) {
	    iter = _relations.keySet().iterator();
	    NameOpValue nov;
	    while(iter.hasNext()) {
		attr = (String)iter.next();
		nov = dstNode.getParam(attr);
		if (nov == null) {
		    dstNode.add( getParam(attr) );
		} else {
		    nov.merge( getParam(attr) );
		}
	    }
	}
	
        if (_bindings != null) {
            iter = _bindings.keySet().iterator();
            Bindings bind;
            while(iter.hasNext()) {
		attr = (String)iter.next();
		bind = dstNode.getBindings(attr);
		if (bind == null) {
		    dstNode.add( getBindings(attr) );
		} else {
		    bind.merge( getBindings(attr) );
		}
            }
        }
	
    }

    /**
     * Returns the relation associated with the given attribute.
     *
     * @param attribute the attribute of the relation.
     * @return the relation for the attribute. Null, if not found.
     */
    public NameOpValue getParam(String attribute) {
	if (_relations == null || attribute == null) return null;
	return (NameOpValue)_relations.get(canonicalize(attribute));
    }
    
    /**
     * Returns the variable definitions associated wit the given
     * attribute.
     *
     * @param attribute the attribute of the variable deinitions.
     * @return the variable deinitions for the attribute. 
     *         Null, if not found.
     */
    public Bindings getBindings(String attribute) {
	if (_bindings == null || attribute == null) return null;
	return (Bindings)_bindings.get(canonicalize(attribute));
    }

    /**
     * Removes a relation for the specified attribute.
     *
     * @param attribute the attribute name for the 
     *        relation to remove.
     * @return the relation that was removed.
     */
    public NameOpValue removeParam(String attribute) {
	if (_relations == null || attribute == null) return null;
	return (NameOpValue)_relations.remove(canonicalize(attribute));
    }
    
    /**
     * Removes a bindings list for the specified attribute.
     *
     * @param attribute the attribute name for the
     *        bindings.
     * @return the bindings that were removed.
     */
    public Bindings removeBindings(String attribute) {
        if (_relations == null || attribute == null) return null;
        return (Bindings)_bindings.remove(canonicalize(attribute));
    }

    /**
     * Returns the relations.
     *
     * @return the map of relations.
     */
    public Map getRelations() {
	return _relations;
    }

    /**
     * Returns the variable definitions.
     *
     * @return the map of variable definitions.
     */
    public Map getBindings() {
	return _bindings;
    }

    /**
     * Evalutes the rsl tree against the specified symbol table.
     * All the variable definitions are first evaluated because
     * they might update the symbol table. Then all the relations
     * followed by the sub-specifications are evaluated.
     *
     * @param symbolTable the symbol table to evalute variables against.
     * @return the evaluated rsl tree.
     * @exception RslEvaluationException If an error occured during 
     *            rsl evaluation.
     */
    public AbstractRslNode evaluate(Map symbolTable) 
	throws RslEvaluationException {
	
	if (symbolTable == null) symbolTable = new HashMap();
	
	Map localSymbolTable = null;
	Iterator iter;
	
	RslNode finalRsl = new RslNode(getOperator());
	
	if (_bindings != null && _bindings.size() > 0) {
	    localSymbolTable = new HashMap(symbolTable);
	    iter = _bindings.keySet().iterator();
	    Bindings binds;
	    while( iter.hasNext() ) {
		binds = getBindings( (String)iter.next() );
		finalRsl.put( binds.evaluate( localSymbolTable ) );
	    }
	} else {
	    localSymbolTable = symbolTable;
	}
	
	if (_relations != null && _relations.size() > 0) {
	    iter = _relations.keySet().iterator();
	    NameOpValue nov;
	    while(iter.hasNext()) {
		nov = getParam( (String)iter.next() );
		finalRsl.put( nov.evaluate(localSymbolTable) );
	    }
	}

	if (_specifications != null && _specifications.size() > 0) {
	    iter = _specifications.iterator();
	    AbstractRslNode node;
	    while(iter.hasNext()) {
		node = (AbstractRslNode)iter.next();
		finalRsl.add( node.evaluate(localSymbolTable) );
	    }
	}
	
	return finalRsl;
    }
    
    /**
     * Produces a RSL representation of node.
     *
     * @param buf buffer to add the RSL representation to.
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     */    
    public void toRSL(StringBuffer buf, boolean explicitConcat) {
	Iterator iter;

	buf.append( getOperatorAsString() );
	
	if (_bindings != null && _bindings.size() > 0) {
	    iter = _bindings.keySet().iterator();
	    Bindings binds;
	    while( iter.hasNext() ) {
		binds = getBindings( (String)iter.next() );
		binds.toRSL(buf, explicitConcat);
	    }
	}
	
	if (_relations != null && _relations.size() > 0) {
	    iter = _relations.keySet().iterator();
	    NameOpValue nov;
	    while(iter.hasNext()) {
		nov = getParam( (String)iter.next() );
		nov.toRSL(buf, explicitConcat);
	    }
	}
	
	if (_specifications != null && _specifications.size() > 0) {
	    iter = _specifications.iterator();
	    AbstractRslNode node;
	    while(iter.hasNext()) {
		node = (AbstractRslNode)iter.next();
		buf.append(" (");
		node.toRSL(buf, explicitConcat);
		buf.append(" )");
	    }
	}
    }
    
}
