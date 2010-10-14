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
public abstract class AbstractRslNode {
    
    public static final int AND   = 1;
    public static final int OR    = 2;
    public static final int MULTI = 3;
    
    protected int _operator;
    protected List _specifications;
    
    public AbstractRslNode() {
	setOperator(AND);
    }
    
    public AbstractRslNode(int operator) {
	setOperator(operator);
    }

    public abstract boolean add(Bindings bindings);
    
    public abstract boolean add(NameOpValue relations);
    
    /**
     * Adds a rsl parse tree to this node.
     *
     * @param node the rsl parse tree to add.
     */
    public boolean add(AbstractRslNode node) {
        if (_specifications == null) _specifications = new LinkedList();
        return _specifications.add(node);
    }

    /**
     * Returns the relation associated with the given attribute.
     *
     * @param attribute the attribute of the relation.
     * @return the relation for the attribute. Null, if not found.
     */
    public abstract NameOpValue getParam(String attribute);

    /**
     * Returns the variable definitions associated wit the given
     * attribute.
     *
     * @param attribute the attribute of the variable deinitions.
     * @return the variable deinitions for the attribute.
     *         Null, if not found.
     */
    public abstract Bindings getBindings(String attribute);
    
    /**
     * Removes a specific sub-specification tree from the
     * sub-specification list.
     *
     * @param node node to remove.
     * @return true if the tree was removed successfuly. False,
     *         otherwise.
     */
    public boolean removeSpecification(AbstractRslNode node) {
        if (_specifications == null || node == null) return false;
        return _specifications.remove(node);
    }

    /**
     * Removes a bindings list for the specified attribute.
     *
     * @param attribute the attribute name for the
     *        bindings.
     * @return the bindings that were removed.
     */
    public abstract Bindings removeBindings(String attribute);

    /**
     * Removes a relation for the specified attribute.
     *
     * @param attribute the attribute name for the
     *        relation to remove.
     * @return the relation that was removed.
     */
    public abstract NameOpValue removeParam(String attribute);

    /**
     * Merges the specified node with the current node.
     * All sub-specifications from the given node will be
     * copied to the current node. All relations and variable
     * definitions will be added together in the current node.
     *
     * @param inNode the source parse tree.
     */
    public void merge(AbstractRslNode inNode) {
	inNode.mergeTo(this);
    }

    public void mergeTo(AbstractRslNode dstNode) {
        Iterator iter = null;
        if (_specifications != null) {
            iter = _specifications.iterator();
            AbstractRslNode node;
            while(iter.hasNext()) {
                node = (AbstractRslNode)iter.next();
                dstNode.add(node);
            }
        }
    }

    /**
     * Returns the list of sub-specifications.
     *
     * @return the list of other sub-specifications.
     */
    public List getSpecifications() {
	return _specifications;
    }

    /**
     * Returns the node operator.
     *
     * @return the operator.
     */
    public int getOperator() {
	return _operator;
    }

    /**
     * Sets the operator.
     *
     * @param oper the operator.
     */
    public void setOperator(int oper) {
	_operator = oper;
    }

    /**
     * Returns the operator as a string.
     *
     * @return operator in a string representation.
     */
    public String getOperatorAsString() {
	return getOperatorAsString(_operator);
    }

    /**
     * Returns a string represention of a given operator.
     *
     * @param op the operator.
     * @return the string representation of the operator.
     */
    public static String getOperatorAsString(int op) {
	switch(op) {
	case AND: 
	    return "&";
	case MULTI: 
	    return "+";
	case OR: 
	    return "|";
	default: 
	    return "??";
	}
    }

    /**
     * Evalutes the rsl tree.
     * All the variable definitions are first evaluated because
     * they might update the symbol table. Then all the relations
     * followed by the sub-specifications are evaluated.
     *
     * @return the evaluated rsl tree.
     * @exception RslEvaluationException If an error occured during 
     *            rsl evaluation.
     */
    public AbstractRslNode evaluate() 
	throws RslEvaluationException {
	return evaluate(null);
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
    public abstract AbstractRslNode evaluate(Map symbolTable)
	throws RslEvaluationException;
    
    /**
     * Returns a RSL representation of this relation. <BR>
     * <I>Note: Enable explicitConcat to generate more 'valid' RSL</I>
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
     * Produces a RSL representation of node.
     *
     * @param buf buffer to add the RSL representation to.
     * @param explicitConcat if true explicit concatination will
     *        be used in RSL strings.
     */    
    public abstract void toRSL(StringBuffer buf, boolean explicitConcat);
    
    public String toString() {
	return toRSL(true);
    }

    /**
     * Canonicalizes a string by removing any underscores and 
     * moving all characters to lowercase.
     *
     * @param str string to canonicalize
     * @return canonicalized string
     */
    public static String canonicalize(String str) {
	if (str == null) return null;
	int length = str.length();
	char ch;
	StringBuffer buf = new StringBuffer(length);
	for (int i=0;i<length;i++) {
	    ch = str.charAt(i);
	    if (ch == '_') continue;
	    buf.append( Character.toLowerCase(ch) );
	}
	return buf.toString();
    }
    
}
