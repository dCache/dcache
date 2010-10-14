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

public class RSLParserTest extends TestCase {

    private Properties validRsls;
    private Properties invalidRsls;
    
    public RSLParserTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(RSLParserTest.class);
    }

    public void setUp() {
	validRsls = new Properties();
	validRsls.put("rsl1", "+(&(executable=myprogram)(stdin<2.4))(&(stdin=8)   \n\n\n(stdin=0))");
	validRsls.put("rsl2", "&(executable=myprogram)(stdin<2.4)(stdin=8)(stdin=0)");
	validRsls.put("rsl3", "&(a=a)(a=b)");
	validRsls.put("rsl4", " &(a=a)");
	validRsls.put("rsl5", "(* dont use this comment *)\n&(string=\"my name is \"\"Nick Karonis\"\" today\") (*or this one*)");
	validRsls.put("rsl6", "&(&(&(&(a=a))))");
	validRsls.put("rsl7", "&(args=\"aa ) bb\")");
	validRsls.put("rsl8", "+(&(executable=myprogram)(stdin<2.4))(|(stdin=8)   \n\n\n(stdin=0))");
	validRsls.put("rsl9", "+(&(executable=myprogram)(stdin<2.4)(|(a=bbb)(yyy=z)))(|(stdin=8)   \n\n\n(stdin=0))");
	validRsls.put("rsl10", "&(args=hello \n\n)");
	validRsls.put("rsl11", "&(executable=\"\")");
	validRsls.put("rsl12", "&(executable=\"abc \"\" \"\" def\")");
	//validRsls.put("rsl13", "args=\"aa ) \"\"bb cc dd\"");
	validRsls.put("rsl14", "&((*comment*)string(*comment*) \n=(*comment\ncontinue comment*)\"my (* ok *) name is \"\"Nick Karonis\"\" today\"(*comment*)) (* or this one *)");
	validRsls.put("rsl15", "&(* dont use this comment *)\n(\n(*comment*)string(*comment*)\n=(*comment\ncontinue comment*)\"my name is \"\"Nick Karonis\"\" today\"(*comment*)) (*or this one*)");
	//validRsls.put("rsl16", "(* dont use this comment *)\n(* comment*) string  (* comment*)\n=(*comment\ncontinue comment*)\"my name is \"\"Nick Karonis\"\" today\"(*comment*) (*or this one*)");
	validRsls.put("rsl17", "+(* dont use this comment *)\n((*comment*)string(*comment*)=(*comment\ncontinue comment*)\"my name is \"\"Nick Karonis\"\" today\"(*comment*)) (*or this one*)");
	validRsls.put("rsl18", "+(string='let''s try ''single quotes'' with \"double too\" ok')");

	invalidRsls = new Properties();

	invalidRsls.put("rsl1", "(my executable=a.out)");
	invalidRsls.put("rsl2", "(executable=/home /a.ou:t#22)");
	invalidRsls.put("rsl3", "&(args=dddd)(executable=)(more_args=ooo)");
	invalidRsls.put("rsl4", "(executable=^$test $$$  quotes$)");
	invalidRsls.put("rsl5", "(executable=^$my value $ \nmore \"\"\" stuff)");
	invalidRsls.put("rsl6", "(executable=)");
	invalidRsls.put("rsl7", "(args=\"\"a\"\"b\")");
	invalidRsls.put("rsl8", "(args=hello \n\nworld)");
	invalidRsls.put("rsl9", "(arguments=\"ccc ddd\"\"zzz\"\")");
	//invalidRsls.put("rsl10", "&(arguments=\"\"\"\"\")(exe=abc)");
	invalidRsls.put("rsl11", "(a=a)");
	invalidRsls.put("rsl12", "+(&(executable=myprogram)(stdin<2.4))(+(stdin=8)   \n\n\n(stdin=0))+");
	invalidRsls.put("rsl13", "(=a.out)");
	invalidRsls.put("rsl14", "(executable=\")");
	invalidRsls.put("rsl15", "(executable=^\")");
	invalidRsls.put("rsl16", "(executable=^/)");
    }
    
    public void testAdvanced() throws Exception {
	String rsl = "&(arguments = -e '$GLOBUS_SH_PERL -e ''print STDERR \"stderr\n\"; '" +
	    "#                      'print STDOUT \"stdout\n\";''')";

	RslNode node = RSLParser.parse(rsl);
	
	NameOpValue nv = null;
	List values;

	nv = node.getParam("ARGUMENTS");
	values = nv.getValues();
	
	assertEquals("arg size",
                     2, 
		     values.size());

	assertEquals("arg 1",
                     "-e", 
		     ((Value)values.get(0)).getValue() );

	String e = "$GLOBUS_SH_PERL -e 'print STDERR \"stderr\n\"; print STDOUT \"stdout\n\";'";

        assertEquals("arg 2",
		     e,
		     ((Value)values.get(1)).getCompleteValue() );
    }

    public void testSlash() throws Exception {
	String rsl;
	RslNode node;

	rsl = "&(executable=/bin/echo)(arguments=\\)";

	node = RSLParser.parse(rsl);
	
	NameOpValue nv = null;
	List values;

	nv = node.getParam("ARGUMENTS");
	values = nv.getValues();

	assertEquals("arg size",
                     1, 
		     values.size());

	assertEquals("arg 1",
		     "\\",
		     ((Value)values.get(0)).getCompleteValue() );

	rsl = "&(executable=/bin/echo)(arguments=\"\\\")";
	
	node = RSLParser.parse(rsl);

	assertEquals("arg size",
                     1, 
		     values.size());

	assertEquals("arg 1",
		     "\\",
		     ((Value)values.get(0)).getCompleteValue() );
    }

    public void testValid() {
	Enumeration e = validRsls.keys();
	String key;
	String rsl;
	while(e.hasMoreElements()) {
	    key = (String)e.nextElement();
	    rsl = validRsls.getProperty(key);
	    
	    System.out.println("Parsing valid rsl " + key + ": " + rsl);
	    try {
		RSLParser.parse(rsl);
	    } catch(Exception ex) {
		ex.printStackTrace();
		fail("Failed to parse!!!");
	    }
	}
    }
    
    public void testInvalid() {
	Enumeration e = invalidRsls.keys();
	String key;
	String rsl;
	while(e.hasMoreElements()) {
	    key = (String)e.nextElement();
	    rsl = invalidRsls.getProperty(key);
	    
	    System.out.println("Parsing invalid rsl " + key + ": " + rsl);
	    try {
		RslNode tree = RSLParser.parse(rsl);
		fail("Failed to catch parse error of " + rsl);
	    } catch(Exception ex) {
	    }
	}
    }

    public void testQuotes() throws Exception {
	String rsl;
	RslNode node;

	rsl = "&(arg1=\"foo\"\"bar\")(arg2='foo''bar')(arg3='')(arg4=\"\")" +
	    "(executable=\"/bin/echo\")(arguments='mis')";
	
	node = RSLParser.parse(rsl);

	testQuotesSub(node);

	rsl = node.toString();
	node = RSLParser.parse(rsl);

	testQuotesSub(node);
    }

    private void testQuotesSub(RslNode node) {
	NameOpValue nv = null;
	List values;

	nv = node.getParam("arg1");
	values = nv.getValues();
	
	assertEquals("arg1",
		     "foo\"bar",
		     ((Value)values.get(0)).getCompleteValue() );

	nv = node.getParam("arg2");
	values = nv.getValues();

	assertEquals("arg2",
		     "foo'bar",
		     ((Value)values.get(0)).getCompleteValue() );

	
	nv = node.getParam("arg3");
	values = nv.getValues();

	assertEquals("arg3",
		     "",
		     ((Value)values.get(0)).getCompleteValue() );

	nv = node.getParam("arg4");
	values = nv.getValues();

	assertEquals("arg4",
		     "",
		     ((Value)values.get(0)).getCompleteValue() );

	nv = node.getParam("executable");
	values = nv.getValues();

	assertEquals("executable",
		     "/bin/echo",
		     ((Value)values.get(0)).getCompleteValue() );
	
	nv = node.getParam("arguments");
	values = nv.getValues();
	
	assertEquals("arguments",
		     "mis",
		     ((Value)values.get(0)).getCompleteValue() );
    }
    
}
