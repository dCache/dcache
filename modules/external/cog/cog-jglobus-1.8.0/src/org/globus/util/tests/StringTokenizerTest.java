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
package org.globus.util.tests;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.globus.util.QuotedStringTokenizer;

public class StringTokenizerTest extends TestCase {

    public StringTokenizerTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(StringTokenizerTest.class);
    }

    public void test1() {
	QuotedStringTokenizer s = new QuotedStringTokenizer("this");
	assertEquals("this", s.nextToken());
    }

    public void test2() {
	QuotedStringTokenizer s = new QuotedStringTokenizer(" this  is test  ");
	assertEquals("this", s.nextToken());
	assertEquals("is", s.nextToken());
	assertEquals("test", s.nextToken());
    }

    public void test3() {
	QuotedStringTokenizer s = new QuotedStringTokenizer("b this  \"is\" test  a");
	assertEquals("b", s.nextToken());
	assertEquals("this", s.nextToken());
	assertEquals("is", s.nextToken());
	assertEquals("test", s.nextToken());
	assertEquals("a", s.nextToken());
    }

    public void test4() {
	QuotedStringTokenizer s = new QuotedStringTokenizer("b this  \"is\" 't\"est'  \"a");
	assertEquals("b", s.nextToken());
	assertEquals("this", s.nextToken());
	assertEquals("is", s.nextToken());
	assertEquals("t\"est", s.nextToken());
	assertEquals("a", s.nextToken());
    }

    public void test5() {
	QuotedStringTokenizer s = new QuotedStringTokenizer("b this  \"jar\\\\ek\" 't\"est'  \"a");
	assertEquals(5, s.countTokens());
	assertEquals("b", s.nextToken());
	assertEquals("this", s.nextToken());
	assertEquals(3, s.countTokens());
	assertEquals("jar\\ek", s.nextToken());
	assertEquals("t\"est", s.nextToken());
	assertEquals(1, s.countTokens());
	assertEquals(true, s.hasMoreTokens());
	assertEquals("a", s.nextToken());
	assertEquals(0, s.countTokens());
	assertEquals(false, s.hasMoreTokens());
	assertEquals(null, s.nextToken());
	assertEquals(false, s.hasMoreTokens());
	assertEquals(null, s.nextToken());
	assertEquals(0, s.countTokens());
    }
}
