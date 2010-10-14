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
package org.globus.gram.tests;

import org.globus.gram.GramAttributes;

import java.util.Map;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;

public class GramAttributesTest extends TestCase {

    protected GramAttributes attribs;

    public GramAttributesTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(GramAttributesTest.class);
    }

    public void setUp() {
	String rsl = "&(executable=/bin/ls)(arguments=-arg1 -arg2 \"-arg3 with space\" \"'arg4 in quotes'\")(directory=/home/gawor)(stdin=https://pitcairn.mcs.anl.gov:9999/test)(environment=(v1 value1/$(JAREK)/value2 ) (v2 $(GLOBUS)) (v3 $(HOME)/data # /bin))";
	
	try {
	    attribs = new GramAttributes(rsl); 
	} catch(Exception e) {
	    fail("Failed to parse rsl");
}
	}
    
    public void testParse() {
	
	assertEquals("executable", 
		     "/bin/ls", attribs.getExecutable());
	assertEquals("directory", 
		     "/home/gawor", attribs.getDirectory());
	assertEquals("stdin",
		     "https://pitcairn.mcs.anl.gov:9999/test",
		     attribs.getStdin());
	assertEquals("stdout",
		     null, attribs.getStdout());

	List args = attribs.getArguments();
	
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

	Map envs = attribs.getEnvironment();
	
	assertEquals("env size",
		     3, envs.size());

	assertEquals("env1",
		     "value1/$(JAREK)/value2", envs.get("v1"));

	assertEquals("env2",
		     "$(GLOBUS)", envs.get("v2"));
	
	assertEquals("env3",
		     "$(HOME)/data/bin", envs.get("v3"));
    }

    public void testModify() {

	attribs.setExecutable("/home/gawor/ls");

	assertEquals("executable", 
		     "/home/gawor/ls", 
		     attribs.getExecutable());
	
	attribs.setStdout("http://goshen.mcs.anl.gov:2222:/kkkk");
	
	assertEquals("stdout",
		     "http://goshen.mcs.anl.gov:2222:/kkkk",
		     attribs.getStdout());

	// modify arg list
	
	assertEquals("delete arg1",
		     true, attribs.deleteArgument("-arg2") );
	
	assertEquals("delete arg2",
		     false, attribs.deleteArgument("-noarg") );
	
	attribs.addArgument("test arg");

	// check arg list

	List args = attribs.getArguments();
	
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
		     true, attribs.deleteEnvVariable("v2") );
	
	assertEquals("delete env2",
		     false, attribs.deleteEnvVariable("v8") );
	
	attribs.addEnvVariable("v5", "value5");

	// check env

	Map envs = attribs.getEnvironment();
	
	assertEquals("env size",
		     3, envs.size());
	
	assertEquals("env1",
		     "value1/$(JAREK)/value2", envs.get("v1"));

	assertEquals("env2",
		     "$(HOME)/data/bin", envs.get("v3"));
	
	assertEquals("env3",
		     "value5", envs.get("v5"));
	
	System.out.println( attribs.toRSL() );
    }
    
}
