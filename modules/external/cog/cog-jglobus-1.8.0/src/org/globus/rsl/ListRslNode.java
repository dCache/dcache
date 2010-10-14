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
 * This class represents an abstract RSL parse tree. It is composed of variable definitions 
 * (bindings), relations, and sub-specifications (sub nodes).
 */
public class ListRslNode extends AbstractRslNode {
    
    protected List _relations = null;
    protected List _bindings = null;
    
    public ListRslNode() {
	super();
    }
    
    public ListRslNode(int operator) {
	super(operator);
    }

    public boolean add(Bindings bindings) {
	if (_bindings == null) {
	    _bindings = new LinkedList();
	}
	return _bindings.add(bindings);
    }
    
    public boolean add(NameOpValue relation) {
	if (_relations == null) {
	    _relations = new LinkedList();
	}
	return _relations.add(relation);
    }
    
    /**
     * Returns the relation associated with the given attribute.
     *
     * @param attribute the attribute of the relation.
     * @return the relation for the attribute. Null, if not found.
     */
    public NameOpValue getParam(String attribute) {
	if (_relations == null || attribute == null) return null;

	Iterator iter = _relations.iterator();
	NameOpValue nv;
	String canonAttrib = canonicalize(attribute);
	String tmpCanonAttrib = null;
	while(iter.hasNext()) {
	    nv = (NameOpValue)iter.next();
	    tmpCanonAttrib = canonicalize(nv.getAttribute());
	    if (tmpCanonAttrib.equalsIgnoreCase(canonAttrib)) {
		return nv;
	    }
	}
	return null;
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

        Iterator iter = _bindings.iterator();
        Bindings bind;
        String canonAttrib = canonicalize(attribute);
        String tmpCanonAttrib = null;
        while(iter.hasNext()) {
            bind = (Bindings)iter.next();
	    tmpCanonAttrib = canonicalize(bind.getAttribute());
            if (tmpCanonAttrib.equalsIgnoreCase(canonAttrib)) {
                return bind;
            }
        }
        return null;
    }
    
    /**
     * Removes a bindings list for the specified attribute.
     *
     * @param attribute the attribute name for the
     *        bindings.
     * @return the bindings that were removed.
     */
    public Bindings removeBindings(String attribute) {
        if (_bindings == null || attribute == null) return null;

        Iterator iter = _bindings.iterator();
        Bindings bind;
        String canonAttrib = canonicalize(attribute);
        String tmpCanonAttrib = null;
        while(iter.hasNext()) {
            bind = (Bindings)iter.next();
            tmpCanonAttrib = canonicalize(bind.getAttribute());
            if (tmpCanonAttrib.equalsIgnoreCase(canonAttrib)) {
		iter.remove();
                return bind;
            }
        }
        return null;
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

        Iterator iter = _relations.iterator();
        NameOpValue nv;
	String canonAttrib = canonicalize(attribute);
        String tmpCanonAttrib = null;
        while(iter.hasNext()) {
            nv = (NameOpValue)iter.next();
            tmpCanonAttrib = canonicalize(nv.getAttribute());
            if (tmpCanonAttrib.equalsIgnoreCase(canonAttrib)) {
		iter.remove();
                return nv;
            }
        }
        return null;
    }

    public void mergeTo(AbstractRslNode dstNode) {
	Iterator iter = null;

	super.mergeTo(dstNode);
	
	if (_relations != null) {
	    iter = _relations.iterator();
	    NameOpValue nov, tmpNov;
	    while(iter.hasNext()) {
		tmpNov = (NameOpValue)iter.next();
		nov = dstNode.getParam(tmpNov.getAttribute());
		if (nov == null) {
		    dstNode.add(tmpNov);
		} else {
		    nov.merge(tmpNov);
		}
	    }
	}
	
	if (_bindings != null) {
	    iter = _bindings.iterator();
	    Bindings bind, tmpBind;
	    while(iter.hasNext()) {
		tmpBind = (Bindings)iter.next();
		bind = dstNode.getBindings(tmpBind.getAttribute());
		if (bind == null) {
		    dstNode.add(tmpBind);
		} else {
		    bind.merge(tmpBind);
		}
	    }
	}
    }

    /**
     * Returns the relations.
     *
     * @return the list of relations.
     */
    public List getRelations() {
	return _relations;
    }

    /**
     * Returns the variable definitions.
     *
     * @return the map of variable definitions.
     */
    public List getBindings() {
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
	Iterator iter = null;

	ListRslNode finalRsl = new ListRslNode(getOperator());

	if (_bindings != null && _bindings.size() > 0) {
	    iter = _bindings.iterator();
	    localSymbolTable = new HashMap(symbolTable);
	    Bindings binds;
	    while( iter.hasNext() ) {
		binds = (Bindings)iter.next();
		finalRsl.add( binds.evaluate( localSymbolTable ) );
	    }
	} else {
	    localSymbolTable = symbolTable;
	}
	
	if (_relations != null && _relations.size() > 0) {
            iter = _relations.iterator();
	    NameOpValue nov;
	    while(iter.hasNext()) {
		nov = (NameOpValue)iter.next();
		finalRsl.add( nov.evaluate(localSymbolTable) );
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
            iter = _bindings.iterator();
            Bindings binds;
            while( iter.hasNext() ) {
                binds = (Bindings)iter.next();
                binds.toRSL(buf, explicitConcat);
            }
        }
        
        if (_relations != null && _relations.size() > 0) {
            iter = _relations.iterator();
            NameOpValue nov;
            while(iter.hasNext()) {
                nov = (NameOpValue)iter.next();
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
