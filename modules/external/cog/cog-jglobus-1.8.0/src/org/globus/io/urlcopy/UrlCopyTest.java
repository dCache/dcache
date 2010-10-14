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
package org.globus.io.urlcopy;

import org.globus.util.GlobusURL;

public class UrlCopyTest {

  private static String [] inurls = { "http://www-unix.mcs.anl.gov/~gawor/index.html",
				      "ftp://charlie.iit.edu/pub/IMC/gleeclub_big.gif",
				      "gsiftp://dg0n1.mcs.anl.gov/testdata/globus_io_common.c" };
 
    private static String outurl = "gsiftp://dg0n1.mcs.anl.gov/testdata/";
    

  private static TransferListener l = new TransferListener();

  public static void run() {

    for (int i=0;i<inurls.length;i++) {
      url2FileCopy(inurls[i], "file:///", i);
    }
    
    for (int i=0;i<inurls.length;i++) {
      url2FileCopy(inurls[i], outurl, i+1);
    }

  }

  private static void url2FileCopy(String fromu, String base, int i) {

    try {

      GlobusURL from = new GlobusURL(fromu);
      GlobusURL to   = new GlobusURL(base + "transfertest." + from.getProtocol() + "." + (i+1));
      
      System.out.println("Copying...");
      System.out.println(" From: " + from.getURL());
      System.out.println("   To: " + to.getURL());
      
      long st = System.currentTimeMillis();

      UrlCopy c = new UrlCopy();
      c.setSourceUrl(from);
      c.setDestinationUrl(to);
      c.setUseThirdPartyCopy(true);
      c.addUrlCopyListener(l);
      c.run();

      long ft = System.currentTimeMillis();
      System.out.println("Done: " + (ft - st));

    } catch(Exception e) {
      System.err.println( "Copy failed:" + e.getMessage());
    }

  }
  
  public static void main(String[] argv) {
    UrlCopyTest.run();
  }
}

class TransferListener implements UrlCopyListener {
    
    Exception _exception;

    public void transfer(long current, long total) {
	if (total == -1) {
	    if (current == -1) {
		System.out.println("<thrid party transfer: progress unavailable>");
	    } else {
		System.out.println(current);
	    }
	} else {
	    System.out.println(current + " out of " + total);
	}
    }
    
    public void transferError(Exception e) {
	_exception = e;
    }

    public void transferCompleted() {
	if (_exception == null) {
	    System.out.println("Transfer completed successfully");
	} else {
	    System.out.println("Transfer failed: " + _exception.getMessage());
	}
    }
    
}
