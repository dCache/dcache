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
package org.globus.mds;

import java.util.*;

import org.globus.common.*;

public class MDSTest2 {

  private int errorCount = 0;

  MDSResult mdsResult;
  
  public void displayAttributes(MDS mds, String dn) {
    try {

      MDSResult mdsResult = mds.getAttributes( dn );      
      mdsResult.print();
      System.out.println();

    } catch (MDSException e) {
      System.err.println("MDS error:" + e.getMessage() + " " + e.getLdapMessage());
      errorCount++;
    }
  }
  
  public void run(String host, String port, String userdn, String userpwd, String bdn) {
    
    String dn;
    MDS mds = new MDS(host, port);
    
    try {

      try {
	mds.connect(userdn, userpwd);
      } catch (MDSException e) {
	System.err.println("MDS error:" + e.getMessage() + " " + e.getLdapMessage());
	errorCount++;
	return;
      }
      
      try {
	mdsResult = mds.getAttributes( bdn );      
      } catch (MDSException e) {
	System.err.println("MDS error:" + e.getMessage() + " " + e.getLdapMessage());
	errorCount++;
	return;
      }
      
      mdsResult.print();
      
      mdsResult.remove("dn");
      mdsResult.add("testfield", new String [] {"value1", "value2", "value3"});
    
      try {
	mds.updateEntry(bdn, mdsResult);
      } catch (MDSException e) {
	System.err.println("MDS error:" + e.getMessage() + " " + e.getLdapMessage());
	errorCount++;
	return;
      }
      
      System.out.println();
      
      displayAttributes(mds, bdn );
      
      MDSResult a3 = new MDSResult();
      a3.add("testfield", "value2");
      
      try {
	mds.deleteValues(bdn, a3);
      } catch (MDSException e) {
	System.err.println("MDS error:" + e.getMessage() + " " + e.getLdapMessage());
	errorCount++;
	return;
      }
      
      displayAttributes(mds, bdn );
      
      try {
	mds.deleteAttribute(bdn, "testfield");
      } catch (MDSException e) {
	System.err.println("MDS error:" + e.getMessage() + " " + e.getLdapMessage());
	errorCount++;
	return;
      }

      displayAttributes(mds,  bdn );
      
    } finally {
      try {
	mds.disconnect();
      } catch (MDSException e) {
      }
    }    
  }
  
  public void printResults() {
    if( errorCount == 0 ) {
	System.out.println( "{test} MDS TEST 2: succeeded" );
    } else {
	System.out.println( "{test} MDS TEST 2: failed -- "
			    + errorCount + " error(s) encountered" );
    }
  }

  public static void main(String[] argv) {
    if (argv.length < 5) {
      System.err.println("Usage: java MDSTest2 host port userdn userpwd dn");
      System.exit(-1);
    }
    
    MDSTest2 mdsTest = new MDSTest2();
    mdsTest.run(argv[0],
		argv[1],
		argv[2],
		argv[3],
		argv[4]);
    mdsTest.printResults();
  }
}












