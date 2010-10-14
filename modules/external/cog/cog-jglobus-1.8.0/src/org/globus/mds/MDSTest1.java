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

public class MDSTest1 {

  private int errorCount = 0;
  MDSResult mdsResult;
  Hashtable v;

  public void run(String host, String port, String dn) {

    MDS mds = new MDS(host, port);
     
    try {

      try {
	mds.connect();
      } catch (MDSException e) {
	System.err.println("MDS error:" + e.getMessage() + " " + e.getLdapMessage());
	errorCount++;
	return;
      }
      
      try {
	mdsResult = mds.getAttributes(dn);
	mdsResult.print();
      } catch (MDSException e) {
	System.err.println("MDS error:" + e.getMessage() + " " + e.getLdapMessage());
	errorCount++;
	return;
      }
      
      System.out.println("---");

      mds.setSearchLimit(10);
      try {
	v = mds.search(dn,
		       "(objectclass=*)", 
		       new String[]{"objectclass", "domainname"}, 
		       MDS.ONELEVEL_SCOPE);
	
	Enumeration e = v.keys();
	while(e.hasMoreElements()) {
	  dn = (String)e.nextElement();
	  mdsResult = (MDSResult)v.get(dn);
	  System.out.println("DN: " + dn);
	  System.out.println("...domain: " + mdsResult.getFirstValue("domainname"));
	}
      } catch (MDSException e) {
	System.err.println("MDS error:" + e.getMessage() + " " + e.getLdapMessage());
	errorCount++;
	return;
      }
    
    } finally {
      try {
	mds.disconnect();
      } catch (MDSException e) {
      }
    }
  }
  
  public void printResults() {
    if( errorCount == 0 ) {
      System.out.println( "\n{test} MDS TEST 1: succeeded" );
    } else {
      System.out.println( "\n{test} MDS TEST 1: failed -- "
			  + errorCount + " error(s) encountered" );
    }
  }    
  
  public static void main( String[] argv ) {
    if (argv.length < 3) {
      System.err.println("Usage: java MDSTest1 host port dn");
      System.exit(-1);
    }
    
    MDSTest1 mdsTest = new MDSTest1();
    mdsTest.run(argv[0],
		argv[1],
		argv[2]);
    mdsTest.printResults();
  }
}









