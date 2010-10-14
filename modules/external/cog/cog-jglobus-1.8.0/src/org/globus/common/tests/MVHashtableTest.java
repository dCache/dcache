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
package org.globus.common.tests;

import org.globus.common.MVHashtable;

import java.util.Vector;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

public class MVHashtableTest extends TestCase {

    protected MVHashtable table;

    public MVHashtableTest(String name) {
	super(name);
    }

    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(MVHashtableTest.class);
    }

    protected void setUp() {
	table = new MVHashtable();
	table.set("key1", "value1");
	table.add("key1", "value2");

	Object [] values = new Object [] {"v1", "v2", "v3", "v4"};
	table.add("key2", values);

	Vector v2 = new Vector();
	v2.addElement("g1");
	v2.addElement("g2");
	v2.addElement("g3");
	table.add("key3", v2);

	table.add("key9", "value1");
	table.add("key9", "value2");
    }
    
    public void testSize() {
	assertEquals(4, table.size());
    }

    public void testSizeOfAttribute() {
	assertEquals("t1", 2, table.size("key1") );
	assertEquals("t2", 4, table.size("key2") );
	assertEquals("t3", 3, table.size("key3") );
	assertEquals("t4", 2, table.size("key9") );
    }

    public void testContainsName() {
	assertTrue("t1", table.containsName("key3") );
	assertTrue("t2", !table.containsName("key4") );
    }

    public void testContains() {
	assertTrue("t1", table.contains("key1", "value1") );
	assertTrue("t2", table.contains("key2", "v4") );
	assertTrue("t3", table.contains("key3", "g2") );

	assertTrue("t4", table.contains("key9", "value2") );
	assertTrue("t5", table.contains("key9", "value1") );
    }

    public void testKeys() {
	Vector keys = table.getKeys();
	assertEquals("t1", 4, keys.size());
	assertTrue("t2", keys.contains("key1") );
	assertTrue("t3", keys.contains("key2") ); 
	assertTrue("t4", keys.contains("key3") );
	assertTrue("t5", keys.contains("key9") );
    }

    public void testGet() {
	assertTrue("t1", table.get("key5") == null );
	Vector values = table.get("key1");
	assertEquals("t2", 2, values.size() );
	assertTrue("t3", values.contains("value1") );
	assertTrue("t4", values.contains("value2") );
    }
    
    public void testGetValueAt() {
	assertEquals("t1", "value1", table.getValueAt("key1", 0));
	assertEquals("t2", "g3", table.getValueAt("key3", 2));
	assertTrue("t3", table.getValueAt("key4", 5) == null );
	assertEquals("t4", "v3", table.getValueAt("key2", 2));
    }
    
    public void testFirstValue() {
	assertEquals("t1", "value1", table.getFirstValue("key1"));
	assertEquals("t2", "v1", table.getFirstValue("key2"));
	assertEquals("t3", "g1", table.getFirstValue("key3"));
    }

    public void testLastValue() {
	assertEquals("t1", "value2", table.getLastValue("key1"));
        assertEquals("t2", "v4", table.getLastValue("key2"));
        assertEquals("t3", "g3", table.getLastValue("key3"));
    }

    public void testRemoveAttirb() {
	Vector v = (Vector)table.remove("key1");
	assertEquals("t1", 2, v.size() );
	assertEquals("t2", 3, table.size());
	assertTrue("t3", v.contains("value1") );
        assertTrue("t4", v.contains("value2") );
    }

    public void testRemoveValue() {
	assertTrue("t1", !table.remove("key3", "g5") );
	assertTrue("t2", table.remove("key3", "g2") );
	assertEquals("t3", 2, table.size("key3") );
    }

    public void testRemoveValueAtIndex() {
	assertEquals("t1", "v3", table.remove("key2", 2) );
	assertEquals("t2", 3, table.size("key2") );
	assertEquals("t3", "v1", table.getFirstValue("key2"));
	assertEquals("t4", "v4", table.getLastValue("key2"));
    }    
}
