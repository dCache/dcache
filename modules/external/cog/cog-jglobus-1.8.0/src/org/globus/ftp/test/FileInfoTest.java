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
package org.globus.ftp.test;

import org.globus.ftp.FileInfo;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileInfoTest extends TestCase {

    private static Log logger = 
	LogFactory.getLog(ReplyTest.class.getName());

    public static void main(String[] argv) {
	junit.textui.TestRunner.run (suite());
    }
    
    public static Test suite() {
	return new TestSuite(FileInfoTest.class);
    }

    public FileInfoTest(String name) {
	super(name);
    }

    public void testRegular() throws Exception {
	FileInfo f1 = new FileInfo("drwxr-xr-x   2      guest  other  1536  Jan 31 15:15  run.bat");
	
	assertEquals(" run.bat", f1.getName());
	assertEquals(true, f1.isDirectory());
	assertEquals(1536, f1.getSize());
	assertEquals("Jan 31", f1.getDate());
	assertEquals("15:15", f1.getTime());
    }

    public void testNoGroup() throws Exception {
	FileInfo f2 = new FileInfo("-rw-rw-r--   1      globus    117579 Nov 29 13:24 AdGriP.pdf");

	assertEquals("AdGriP.pdf", f2.getName());
	assertEquals(false, f2.isDirectory());
	assertEquals(117579, f2.getSize());
	assertEquals("Nov 29", f2.getDate());
	assertEquals("13:24", f2.getTime());
    }

    public void testSpace() throws Exception {
	FileInfo f3 = new FileInfo("drwxrwxr-x    2 gawor    globus   512 Dec 26  2001 gatekeeper file 2");
	
	assertEquals("gatekeeper file 2", f3.getName());
	assertEquals(true, f3.isDirectory());
	assertEquals(512, f3.getSize());
	assertEquals("Dec 26", f3.getDate());
	assertEquals("2001", f3.getTime());
    }

    public void testFileWithDate() throws Exception {
	FileInfo f4 = new FileInfo("drwxrwxr-x    2 gawor    globus   512 Dec 26  2001 gatekeeper-2001");
	
	assertEquals("gatekeeper-2001", f4.getName());
	assertEquals(true, f4.isDirectory());
	assertEquals(512, f4.getSize());
	assertEquals("Dec 26", f4.getDate());
	assertEquals("2001", f4.getTime());
    }

    public void testFileWithDateWithSpace() throws Exception {
	FileInfo f4 = new FileInfo("drwxrwxr-x    2 gawor    globus   512 Dec 26  2001 gatekeeper-2001   a b c  ");
	
	assertEquals("gatekeeper-2001   a b c  ", f4.getName());
	assertEquals(true, f4.isDirectory());
	assertEquals(512, f4.getSize());
	assertEquals("Dec 26", f4.getDate());
	assertEquals("2001", f4.getTime());
    }

    public void testRegular2() throws Exception {
	FileInfo f4 = new FileInfo("drwxrwxr-x   2 7        7            4096 May  1  1994 bin");
	
	assertEquals("bin", f4.getName());
	assertEquals(true, f4.isDirectory());
	assertEquals(4096, f4.getSize());
	assertEquals("May 1", f4.getDate());
	assertEquals("1994", f4.getTime());
    }

    public void testSoftLink() throws Exception {
	FileInfo f4 = new FileInfo("lrwxrwxrwx    1 root     root           10 Nov  2  2001 mouse -> /dev/psaux");
	assertEquals(true, f4.isSoftLink());
	assertEquals("mouse -> /dev/psaux", f4.getName());
	assertEquals(10, f4.getSize());
	assertEquals("Nov 2", f4.getDate());
	assertEquals("2001", f4.getTime());
    }

    /**
       character device.
       an entry of /dev directory has slightly different format
    */
    public void testCharDev() throws Exception {
	FileInfo f5 = new FileInfo("crw-rw-rw-    1 root     tty        3,  24 Apr 14  2001 ttyq8");
	assertEquals(true, f5.isDevice());
	assertEquals(FileInfo.UNKNOWN_STRING,f5.getName());
	assertEquals(FileInfo.UNKNOWN_STRING,f5.getDate());
	assertEquals(FileInfo.UNKNOWN_STRING,f5.getTime());
	assertEquals(FileInfo.UNKNOWN_NUMBER,f5.getSize());
    }
    /**
       block device.
       an entry of /dev directory has slightly different format
    */
    public void testBlockDev() throws Exception {
	FileInfo f5 = new FileInfo("brw-rw----    1 root     cdrom     15,   0 Apr 14  2001 sonycd");
	assertEquals(true, f5.isDevice());
	assertEquals(FileInfo.UNKNOWN_STRING,f5.getName());
	assertEquals(FileInfo.UNKNOWN_STRING,f5.getDate());
	assertEquals(FileInfo.UNKNOWN_STRING,f5.getTime());
	assertEquals(FileInfo.UNKNOWN_NUMBER,f5.getSize());
    }
    /**
       suppose that group is missing in /dev directory entry
     */
    public void testCharDev2() throws Exception {
	FileInfo f5 = new FileInfo("crw-rw-rw-    1 root             3,  24 Apr 14  2001 ttyq8");
	assertEquals(true, f5.isDevice());
	assertEquals(FileInfo.UNKNOWN_STRING,f5.getName());
	assertEquals(FileInfo.UNKNOWN_STRING,f5.getDate());
	assertEquals(FileInfo.UNKNOWN_STRING,f5.getTime());
	assertEquals(FileInfo.UNKNOWN_NUMBER,f5.getSize());
    }

    public void testPermission() throws Exception { 
        FileInfo f1 = new FileInfo("-rwxrw-r--   2      guest  other  1536  Jan 31 15:15  run.bat"); 
        int mode = (1 << 8) + (1 << 7) + (1 << 6)  +  (1 << 5) + (1 << 4)  +  (1 << 2); 
        assertEquals(mode,f1.getMode()); 
        assertEquals("764",f1.getModeAsString()); 
        assertEquals(true,f1.userCanRead()); 
        assertEquals(true,f1.userCanWrite()); 
        assertEquals(true,f1.userCanExecute()); 
        assertEquals(true,f1.groupCanRead()); 
        assertEquals(true,f1.groupCanWrite()); 
        assertEquals(false,f1.groupCanExecute()); 
        assertEquals(true,f1.allCanRead()); 
        assertEquals(false,f1.allCanWrite()); 
        assertEquals(false,f1.allCanExecute()); 
 
        f1 = new FileInfo("------x-wx   2      guest  other  1536  Jan 31 15:15 run.bat"); 
        mode = (1 << 3) +  (1 << 1) + (1 << 0); 
        assertEquals(mode,f1.getMode()); 
        assertEquals("013",f1.getModeAsString()); 
        assertEquals(false,f1.userCanRead()); 
        assertEquals(false,f1.userCanWrite()); 
        assertEquals(false,f1.userCanExecute()); 
        assertEquals(false,f1.groupCanRead()); 
        assertEquals(false,f1.groupCanWrite()); 
        assertEquals(true,f1.groupCanExecute()); 
        assertEquals(false,f1.allCanRead()); 
        assertEquals(true,f1.allCanWrite()); 
        assertEquals(true,f1.allCanExecute()); 
    } 

    /*
    public void test() throws Exception {
	String b = "drwxrwxr-x   2 7        7            4096 May  1  1994 bin\n" +
	    "drwxrwx-wx  16 468      861          4096 Oct  2 15:19 chammp\n" +
	    "drwxrws-wx   2 1487     1123         4096 Oct 28  1999 chemio\n" +
	    "drwxrwxr-x   2 7        7            4096 Mar  1  1994 dev\n" +
	    "dr-xrwxr-x   2 7        7            4096 Oct 25 16:00 etc\n" +
	    "drwxrws-wx   7 7        300          4096 Nov 14 07:03 incoming\n" +
	    "drwxr-xr-x   2 0        0            4096 Sep 27 19:23 lib\n" +
	    "drwxr-xr-x   2 0        0            4096 Oct  9  1999 lost+found\n" +
	    "drwxr-xr-x   2 793      76           4096 Apr 17  2002 openpbs\n" +
	    "lrwxrwxrwx   1 0        0              21 Sep 20 04:39 pieper -> pub/People/pieper/old\n" +
	    "drwxrwxr-x  72 7        7            4096 Oct 25 15:58 pub\n" +
	    "drwxrwxr-x   3 7        7            4096 Mar  1  1994 usr\n" +
	    "drwxr-sr-x   2 793      76           4096 May 10  1999 wiki\n";
    
	java.io.BufferedReader r = new java.io.BufferedReader(new java.io.StringReader(b));
	String line;
	while( (line = r.readLine()) != null) {
	    System.out.println(line);
	    new FileInfo(line);
	}
	       
    }
    */

}
