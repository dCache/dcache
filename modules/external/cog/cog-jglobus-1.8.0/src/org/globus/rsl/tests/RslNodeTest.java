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
package org.globus.rsl.tests;

import org.globus.rsl.*;

import java.util.*;
import java.io.*;

import junit.framework.*;
import junit.extensions.*;

public class RslNodeTest extends TestCase {

    protected RslNode rslTree;

    public RslNodeTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(RslNodeTest.class);
    }

    public void setUp() {
	String rsl = "&(exECutable=/bin/ls)(arGUments=-arg1 -arg2 \"  -arg3 \")(directory=/home/gawor)(stdin=https://pitcairn.mcs.anl.gov:9999/test)(environment=(v1 value1/$(JAREK)/value2 ) (v2 $(GLOBUS)) (v3 $(HOME)/data # /bin))";
	
	try {
	    rslTree = RSLParser.parse(rsl);
	} catch(Exception e) {
	    fail("Failed to parse rsl");
	}
    }
    
    public void testMerge() {

	RslNode node = null;

	String rsl2 = "&(rslSubSTitution=(var1 value1))(arguments=\" -end\")(stdout=stdout.file)";
	
	try {
	    node = RSLParser.parse(rsl2);
	} catch(Exception e) {
	    fail("Failed to parse rsl");
	}

	// merge the rsl
	rslTree.merge(node);

	NameOpValue nv = null;
	List values;

	nv = rslTree.getParam("ARGUMENTS");
	values = nv.getValues();

        assertEquals("arg size",
                     4, 
		     values.size());

        assertEquals("arg 1",
                     "-arg1", 
		     ((Value)values.get(0)).getValue() );

        assertEquals("arg 2",
                     "-arg2", 
		     ((Value)values.get(1)).getValue() );

        assertEquals("arg 3",
                     "  -arg3 ", 
		     ((Value)values.get(2)).getValue() );

        assertEquals("arg 4",
                     " -end", 
		     ((Value)values.get(3)).getValue() );


	nv = rslTree.getParam("stdout");
	values = nv.getValues();

	assertEquals("stdout size",
		     1,
		     values.size());

	assertEquals("stdout value",
		     "stdout.file",
		     ((Value)values.get(0)).getValue() );


	Bindings db = rslTree.getBindings("rsl_substitutION");

	assertTrue("bindings null", (db != null));

	values = db.getValues();

	assertEquals("rslsubsitution size",
		     1,
		     values.size() );

	assertEquals("rslsub variable",
		     "var1",
		     ((Binding)values.get(0)).getName());

	assertEquals("rslsub variable value",
		     "value1",
		     ((Binding)values.get(0)).getValue().getValue());

	System.out.println("the final rsl:" + rslTree);
    }

    public void testCreate() {

	RslNode rslTree = new RslNode(RslNode.AND);

	Bindings bindings = null;
	NameOpValue nv = null;
	List values = null;

	rslTree.put(new NameOpValue("executable", 
				    NameOpValue.EQ, 
				    "/usr/local/bin/ls"));

	rslTree.put(new NameOpValue("executable",
				    NameOpValue.EQ,
				    "/bin/ls"));

	rslTree.put(new NameOpValue("myMemory", 
				    NameOpValue.LTEQ, 
				    "5"));
	
	rslTree.put(new NameOpValue("arguments", 
				    NameOpValue.EQ, 
				    new String [] {"-l", "-p", " -o "}));

	bindings = new Bindings("rsl_substitution");
	bindings.add(new Binding("var1", "value1"));
	bindings.add(new Binding("var2", "value2"));
	bindings.add(new Binding("var3", "value3"));
	
	rslTree.put(bindings);
	
	// test stuff
	
        nv = rslTree.getParam("ARGUMENTS");
        values = nv.getValues();
	
        assertEquals("arg size",
                     3,
                     values.size());
	
        assertEquals("arg 1",
                     "-l",
                     ((Value)values.get(0)).getValue() );
	
        assertEquals("arg 2",
                     "-p",
                     ((Value)values.get(1)).getValue() );

        assertEquals("arg 3",
                     " -o ",
                     ((Value)values.get(2)).getValue() );

	// remove some args...
	
	assertEquals("remove arg",
		     false, nv.remove(new Value("-p ")));
	
	assertEquals("remove arg2",
		     true, nv.remove(new Value("-p")));

	// test the args again...

        assertEquals("arg size",
                     2,
                     values.size());
	
        assertEquals("arg 1",
                     "-l",
                     ((Value)values.get(0)).getValue() );

        assertEquals("arg 2",
                     " -o ",
                     ((Value)values.get(1)).getValue() );
	
	// test the executable..

	nv = rslTree.getParam("EXECUTABLE");
	
	assertEquals("executable",
                     "/bin/ls",
		     ((Value)nv.getFirstValue()).getValue() );
	
        nv = rslTree.getParam("MY_MEMORY");
	
        assertEquals("myMemory",
                     "5",
                     ((Value)nv.getFirstValue()).getValue() );

	assertEquals("myMemory operator",
		     NameOpValue.LTEQ,
		     nv.getOperator());

	// test bindings

	bindings = rslTree.getBindings("rslSUBstitution");

	assertTrue("bindings null", (bindings != null));

        values = bindings.getValues();

        assertEquals("bind size",
                     3,
                     values.size());
	
        assertEquals("bind 1",
                     "var1",
                     ((Binding)values.get(0)).getName() );
	
        assertEquals("bind 2",
                     "var2",
                     ((Binding)values.get(1)).getName() );
	
        assertEquals("bind 3",
                     "var3",
                     ((Binding)values.get(2)).getName() );
	
    }

    public void testRemove() {

        RslNode node = null;
	
        String rsl2 = "&(\"rsl_SubSTitution\"=(var1 value1))(arguments=\" -end\")(stdout=stdout.file)(stdout=ptys)";
	
        try {
            node = RSLParser.parse(rsl2);
        } catch(Exception e) {
            fail("Failed to parse rsl");
        }

        NameOpValue nv = null;
	Bindings bindings = null;
        List values;

	nv = node.removeParam("stdout");
        values = nv.getValues();
	
        assertEquals("stdout",
                     "ptys",
                     ((Value)values.get(0)).getValue() );
	
	assertEquals("stdout",
                     null,
		     node.removeParam("stdout"));

        bindings = node.removeBindings("rslsubstitutION");
        values = bindings.getValues();

        assertEquals("rsl subst.",
                     "var1",
                     ((Binding)values.get(0)).getName() );
	
        assertEquals("rsl subst.",
                     null,
                     node.removeBindings("rsl_substiTution"));
    }

    public void testEvaluate() {

	String rsl = 
	    " + " +
	    "(& " +
	    "(directory = $(TOPDIR))" +
	    "(executable = $(VAR1))" +
	    ")" +
	    "(&" +
	    "(rsl_substitution  = (TOPDIR  \"/home/nobody\")" +
	    "(DATADIR $(TOPDIR)\"/data\") " +
	    "(EXECDIR $(TOPDIR)/bin) )" +
	    "(executable = $(EXECDIR)/a # .out" +
	    "(* ^-- implicit concatenation *))" +
	    "(directory  = $(TOPDIR) )" +
	    "(arguments  = $(DATADIR)/file1\n" +
	    "(* ^-- implicit concatenation *)" +
	    "$(DATADIR) # /file2\n" +
	    "(* ^-- explicit concatenation *)" +
	    "'$(FOO)'            (* <-- a quoted literal *))" +
	    "(environment = (DATADIR $(DATADIR)))" +
	    "(count = 1)" +
	    ")";
	
	RslNode tree = null;
	try {
	    tree = RSLParser.parse(rsl);
	} catch(Exception e) {
	    fail("Rsl failed to parse!");
	}

	System.out.println( tree.toRSL(true) );

	// null is the symbol table
	Properties p = new Properties();
	p.put("VAR1", "testValue1");
	p.put("TOPDIR", "/home/gawor");
	
	AbstractRslNode finalRsl = null;
	
	try {
	    finalRsl = tree.evaluate(p);
	} catch (RslEvaluationException e) {
	    fail("failed to evaluate rsl!");
	}
	
	System.out.println();
	System.out.println( finalRsl.toRSL(true) );

	NameOpValue nv = null;
	List values    = null;
	
	List specs = finalRsl.getSpecifications();
	
	// this should be the first one...
	finalRsl = (RslNode)specs.get(0);

	assertTrue("rsl node 0 null", (finalRsl != null));
	
	nv = finalRsl.getParam("executable");
	values = nv.getValues();

	assertEquals("executable",
		     "testValue1",
		     ((Value)values.get(0)).getValue());

	nv = finalRsl.getParam("directory");
	values = nv.getValues();

	assertEquals("directory",
		     "/home/gawor",
		     ((Value)values.get(0)).getValue());
	
	// this should be the second one...
	finalRsl = (RslNode)specs.get(1);

	assertTrue("rsl node 1 null", (finalRsl != null));

	nv = finalRsl.getParam("executable");
        values = nv.getValues();

        assertEquals("executable",
                     "/home/nobody/bin/a.out",
                     ((Value)values.get(0)).getValue());

        nv = finalRsl.getParam("directory");
        values = nv.getValues();

        assertEquals("directory",
                     "/home/nobody",
                     ((Value)values.get(0)).getValue());
	
	nv = finalRsl.getParam("arguments");
        values = nv.getValues();

	assertEquals("arg1",
		     "/home/nobody/data/file1",
		     ((Value)values.get(0)).getValue());

	assertEquals("arg2",
		     "/home/nobody/data/file2",
		     ((Value)values.get(1)).getValue());

	assertEquals("arg3",
		     "$(FOO)",
		     ((Value)values.get(2)).getValue());

	nv = finalRsl.getParam("environment");
        values = nv.getValues();

	values = (List)values.get(0);

	assertEquals("env name",
		     "DATADIR",
		     ((Value)values.get(0)).getValue());

	assertEquals("env value",
		     "/home/nobody/data",
		     ((Value)values.get(1)).getValue());

	Bindings bindings = finalRsl.getBindings("rsl_substitution");

	assertTrue("bindings null", (bindings != null));

	values = bindings.getValues();
	
	assertEquals("bind1: name",
		     "TOPDIR",
		     ((Binding)values.get(0)).getName());

        assertEquals("bind1: value",
                     "/home/nobody",
                     ((Binding)values.get(0)).getValue().getValue());

	assertEquals("bind2: name",
                     "DATADIR",
                     ((Binding)values.get(1)).getName());

        assertEquals("bind2: value",
                     "/home/nobody/data",
                     ((Binding)values.get(1)).getValue().getValue());

	assertEquals("bind3: name",
                     "EXECDIR",
                     ((Binding)values.get(2)).getName());

        assertEquals("bind3: value",
                     "/home/nobody/bin",
                     ((Binding)values.get(2)).getValue().getValue());

    }

    
    
}





