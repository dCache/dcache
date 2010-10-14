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

public class RslAttributesTest extends TestCase {

    protected RslAttributes attribs;

    public RslAttributesTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(RslAttributesTest.class);
    }

    public void setUp() {
	String rsl = "&(rsl_substitution=(HOME /home/gawor)(VAR2 testValue))(exECutable=/bin/ls)(arGUments=-arg1 -arg2 \"-arg3 with space\" \"'arg4 in quotes'\")(directory=/home/gawor)(stdin=https://pitcairn.mcs.anl.gov:9999/test)(environment=(v1 value1/$(JAREK)/value2 ) (v2 $(GLOBUS)) (v3 $(HOME)/data # /bin))";
	
	try {
	    attribs = new RslAttributes(rsl); 
	} catch(Exception e) {
	    fail("Failed to parse rsl");
}
	}
    
    public void testParse() {
	
	assertEquals("executable", 
		     "/bin/ls", attribs.getSingle("executable"));
	
	assertEquals("directory", 
		     "/home/gawor", attribs.getSingle("directory"));

	assertEquals("stdin",
		     "https://pitcairn.mcs.anl.gov:9999/test",
		     attribs.getSingle("stdin"));

		     assertEquals("stdout",
				  null, attribs.getSingle("stdout"));

	List args = attribs.getMulti("arguments");
	
	assertEquals("arg size",
		     4, args.size());
	
	assertEquals("arg 1",
		     "-arg1", args.get(0));
	
	assertEquals("arg 2",
		     "-arg2", args.get(1));
	
	assertEquals("arg 3",
		     "-arg3 with space", args.get(2));
	
	assertEquals("arg 4",
		     "'arg4 in quotes'", args.get(3));

	Map envs = attribs.getMap("environment");
	
	assertEquals("env size",
		     3, envs.size());

	assertEquals("env1",
		     "value1/$(JAREK)/value2", envs.get("v1"));

	assertEquals("env2",
		     "$(GLOBUS)", envs.get("v2"));
	
	assertEquals("env3",
		     "$(HOME)/data/bin", envs.get("v3"));

	Map vars = attribs.getVariables("rsl_substitution");

	assertEquals("var name 1",
		     true, vars.containsKey("HOME"));

	assertEquals("var name 2",
		     true, vars.containsKey("VAR2"));

	assertEquals("var name 3",
		     false, vars.containsKey("home"));


        assertEquals("var value 1",
		     "/home/gawor",
		     vars.get("HOME"));

        assertEquals("var value 2",
		     "testValue",
		     vars.get("VAR2"));
    }

    public void testModify() {

	attribs.set("executabLE", "/home/gawor/ls");

	assertEquals("executable", 
		     "/home/gawor/ls", 
		     attribs.getSingle("executable"));
	
	attribs.set("stdout", "http://goshen.mcs.anl.gov:2222:/kkkk");
	
	assertEquals("stdout",
		     "http://goshen.mcs.anl.gov:2222:/kkkk",
		     attribs.getSingle("stdOUT"));

	// modify arg list
	
	assertEquals("delete arg1",
		     true, attribs.remove("arguments", "-arg2") );
	
	assertEquals("delete arg2",
		     false, attribs.remove("arguments", "-noarg") );
	
	attribs.add("arguments", "test arg");

	// check arg list

	List args = attribs.getMulti("arguments");

	assertEquals("arg size",
		     4, args.size());
	
	assertEquals("arg 1",
		     "-arg1", args.get(0));
	
	assertEquals("arg 2",
		     "-arg3 with space", args.get(1));
	
	assertEquals("arg 3",
		     "'arg4 in quotes'", args.get(2));

	assertEquals("arg 4",
		     "test arg", args.get(3));
	
	// modify env

	assertEquals("delete env1",
		     true, attribs.removeMap("environment", "v2") );
	
	assertEquals("delete env2",
		     false, attribs.removeMap("environment", "v8") );
	
	attribs.addMulti("environment", new String [] {"v5", "value5"});
	
	// check env

	Map envs = attribs.getMap("environment");
	
	assertEquals("env size",
		     3, envs.size());
	
	assertEquals("env1",
		     "value1/$(JAREK)/value2", envs.get("v1"));

	assertEquals("env2",
		     "$(HOME)/data/bin", envs.get("v3"));
	
	assertEquals("env3",
		     "value5", envs.get("v5"));

	
	// modify variables

	assertEquals("var remove",
		     false, attribs.removeVariable("rsl_substitution", "VAR1"));

	assertEquals("var remove",
		     true, attribs.removeVariable("rsl_substitution", "VAR2"));
	
	attribs.addVariable("rsl_substitution", "VAR3", "variable3");


	// check variables 

        Map vars = attribs.getVariables("rsl_substitution");

        assertEquals("var name 1",
                     true, vars.containsKey("HOME"));

        assertEquals("var name 2",
                     false, vars.containsKey("VAR2"));

        assertEquals("var name 3",
                     true, vars.containsKey("VAR3"));

        assertEquals("var value 1",
                     "/home/gawor",
                     vars.get("HOME"));

        assertEquals("var value 2",
                     "variable3",
                     vars.get("VAR3"));

	System.out.println( attribs.toRSL() );
    }
    
}
