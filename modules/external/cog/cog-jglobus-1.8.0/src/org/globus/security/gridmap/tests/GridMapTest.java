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
package org.globus.security.gridmap.tests;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.io.IOException;
import java.io.ByteArrayInputStream;

import org.globus.security.gridmap.GridMap;
import org.globus.security.gridmap.GridMapLocal;

public class GridMapTest extends TestCase {

    private static final String GRIDMAP_1 =
	"\"/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Katarzyna (Kate) Keahey\" keahey,kate\r\n" +
	"# this is a comment\r\n" +
	"           \r\n" + 
	"\"/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak\" dvorak2\r\n" + 
	"\"/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak\" dvorak\n"+
        "\"/DC=org/DC=doe grids/OU=People/UID=32845324/Email=john@doe.com/E=john@doe.com\"  jdoe,doej\n" +
        "\"/DC=org/DC=doe grids/OU=People/UID=3284532/USERID=7878/UID=8989/E=john@doe.com\" john_doe\n" +
        "\"/DC=org/DC=doe grids/OU=People/UID=32845/Email=john@doe.com/Email=another@doe.edu\"  doe\n" +
        "\"/DC=org/DC=doegrids/OU=Services/Email=admin@mcs/CN=host/effable.mcs.anl.gov\" host\n" +
        "user1DN user1\n" +
        "\ttestDN\tuser1\n";
    
    private static final String GRIDMAP_2 =
	"\"/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Katarzyna (Kate) Keahey\"\r\n" +
	"# this is a comment\r\n" +
	"           \r\n" + 
	"\"/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak\" dvorak\r\n" + 
	"dvorak\n";

    private static final String GRIDMAP_3 = 
        "\"/O=myCA/CN=\\\"Foo Bar\\\"\" account\r\n";

    private static final String GRIDMAP_4 =
        "/C=DE/ST=Baden-Württemberg/O=University of Stuttgart/OU=HLRS/CN=Oliver Mangold/E=o.mangold@gmx.de test1";

    public GridMapTest(String name) {
	super(name);
    }
    
    public static void main (String[] args) {
	junit.textui.TestRunner.run (suite());
    }

    public static Test suite() {
	return new TestSuite(GridMapTest.class);
    }

    public void test1() {
	
	GridMap map = new GridMap();



        

	try {
        map.load(new ByteArrayInputStream(GRIDMAP_4.getBytes()));
        System.out.println("Test 1 " + map.getUserID("test1"));
	    assertTrue(map.load(new ByteArrayInputStream(GRIDMAP_1.getBytes())));
	} catch (IOException e) {
	    fail(e.getMessage());
	    return;
	}

        assertEquals("user1", map.getUserID("user1DN"));
        assertEquals("user1", map.getUserID("testDN"));
	assertEquals("keahey", 
		     map.getUserID("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Katarzyna (Kate) Keahey"));

	assertEquals("dvorak2", 
		     map.getUserID("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak"));

	assertEquals(null, 
		     map.getUserID("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Jarek Gawor"));

        assertEquals("jdoe", map.getUserID("/DC=org/DC=doe grids/OU=People/UID=32845324/Email=john@doe.com/E=john@doe.com"));

        assertEquals("jdoe", map.getUserID("/DC=org/DC=doe grids/OU=People/USERID=32845324/Email=john@doe.com/E=john@doe.com"));

        assertEquals("john_doe", map.getUserID("/DC=org/DC=doe grids/OU=People/UID=3284532/USERID=7878/UID=8989/E=john@doe.com"));

        assertEquals("john_doe", map.getUserID("/DC=org/DC=doe grids/OU=People/UID=3284532/UID=7878/UID=8989/E=john@doe.com"));

        assertEquals("john_doe", map.getUserID("/DC=org/DC=doe grids/OU=People/UID=3284532/USERID=7878/UID=8989/EMAIL=john@doe.com"));

        assertEquals("doe", map.getUserID("/DC=org/DC=doe grids/OU=People/UID=32845/Email=john@doe.com/e=another@doe.edu"));

        assertEquals("doe", map.getUserID("/DC=org/DC=doe grids/OU=People/UID=32845/e=john@doe.com/Email=another@doe.edu"));


        assertEquals("host", map.getUserID("/DC=org/DC=doegrids/OU=Services/Email=admin@mcs/CN=host/effable.mcs.anl.gov"));
        
	String [] rs = null;
	String [] expected = null;
        
        rs = map.getUserIDs("/DC=org/DC=doe grids/OU=People/UID=32845324/Email=john@doe.com/E=john@doe.com");
        expected = new String[] { "jdoe", "doej" };

	assertEquals(expected.length, rs.length);
	for (int i=0;i<expected.length;i++) {
	    assertEquals(expected[i], rs[i]);
	}
                     
	rs = map.getUserIDs("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Katarzyna (Kate) Keahey");
	expected = new String [] {"keahey", "kate"};

	assertEquals(expected.length, rs.length);
	for (int i=0;i<expected.length;i++) {
	    assertEquals(expected[i], rs[i]);
	}

	rs = map.getUserIDs("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak");
	expected = new String [] {"dvorak2", "dvorak"};
	
        assertEquals(expected.length, rs.length);
        for (int i=0;i<expected.length;i++) {
            assertEquals(expected[i], rs[i]);
        }
	
    }
    
    public void testLocalLookup() {
	GridMap map = new GridMapLocal();

	String username = System.getProperty("user.name");

	assertEquals(username, map.getUserID("whatever") );

	System.setProperty("user.name", "ROot");

	assertEquals(null, map.getUserID("whatever") );

	System.setProperty("user.name", "AdminISTRATOR");

	assertEquals(null, map.getUserID("whatever") );

	System.setProperty("user.name", username);

	assertEquals(username, map.getUserID("whatever2") );
    }

    public void test3() {
	
	GridMap map = new GridMap();

	try {
	    map.load(new ByteArrayInputStream(GRIDMAP_1.getBytes()));   
	} catch (IOException e) {
	    fail(e.getMessage());
	    return;
	}

	assertEquals(true,
		     map.checkUser("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Katarzyna (Kate) Keahey",
				   "keahey"));

	assertEquals(true,
		     map.checkUser("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Katarzyna (Kate) Keahey",
				   "kate"));

	assertEquals(true,
		     map.checkUser("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak",
				   "dvorak"));

	assertEquals(false,
		     map.checkUser("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak",
				   "dvorakkkkk"));

	assertEquals(false,
		     map.checkUser("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorakkkk",
				   "dvorak"));
	
	assertEquals(false, 
		     map.checkUser("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Jarek Gawor", 
				   "gawor"));
    }    

    public void test4() {
	
	GridMap map = new GridMap();

	try {
	    map.load(new ByteArrayInputStream(GRIDMAP_1.getBytes()));   
	} catch (IOException e) {
	    fail(e.getMessage());
	    return;
	}
	
	assertEquals("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak",
		     map.getGlobusID("dvorak"));

	assertEquals("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Katarzyna (Kate) Keahey",
		     map.getGlobusID("keahey"));

	assertEquals("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Katarzyna (Kate) Keahey",
		     map.getGlobusID("kate"));

	assertEquals(null, map.getGlobusID("gawor"));

    }

    public void test5() {
	  
	GridMap map = new GridMap();

	try {
	    map.load(new ByteArrayInputStream(GRIDMAP_1.getBytes()));   
	} catch (IOException e) {
	    fail(e.getMessage());
	    return;
	}

	map.map("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Jarek Gawor", 
		"gawor");

	assertEquals("gawor", 
		     map.getUserID("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Jarek Gawor"));

	map.map("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak",
		"dvorak2");

	assertEquals(true,
		     map.checkUser("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak",
				   "dvorak"));
	
	assertEquals(true,
		     map.checkUser("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak",
				   "dvorak2"));
	
    }

    public void testIgnoreErrors() {
	  
	GridMap map = new GridMap();

	try {
	    map.load(new ByteArrayInputStream(GRIDMAP_2.getBytes()));   
            fail("did not throw exception");
	} catch (IOException e) {
	}

        assertTrue(map.getUserID("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak") == null);
        
        map.setIgnoreErrors(false);

        try {
	    map.load(new ByteArrayInputStream(GRIDMAP_2.getBytes()));   
            fail("did not throw exception");
	} catch (IOException e) {
	}

        assertTrue(map.getUserID("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak") == null);

        map.setIgnoreErrors(true);

        try {
	    assertFalse(map.load(new ByteArrayInputStream(GRIDMAP_2.getBytes())));
	} catch (IOException e) {
            e.printStackTrace();
            fail("Unexpected error");
	}

        assertEquals("dvorak", 
                     map.getUserID("/O=Grid/O=Globus/OU=mcs.anl.gov/CN=Michael Dvorak"));
    }

    public void test6() throws Exception {

        GridMap map = new GridMap();
        
	try {
	    map.load(new ByteArrayInputStream(GRIDMAP_3.getBytes()));   
	} catch (IOException e) {
	    fail(e.getMessage());
	    return;
	}

        assertTrue((map.getAllGlobusID("account").length) == 1);
        String[] globusId = map.getAllGlobusID("account");
        System.out.println("** " + globusId[0]);
        assertEquals("account", map
                     .getUserID("/O=myCA/CN=\"Foo Bar\""));
    }
}

