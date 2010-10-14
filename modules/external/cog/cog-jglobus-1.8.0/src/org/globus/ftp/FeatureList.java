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
package org.globus.ftp;

import java.util.Vector;
import java.util.StringTokenizer;

/**
   Represents features supported by server (as returned by FEAT command).
   Use the static members of this class to refer to well known feature names.
   Example: check if the server supports PARALLEL feature:
   <pre>
   FeatureList fl = new FeatureList(client.getFeatureList());
   if (fl.contains(FeatureList.PARALLEL)) {
       ...
   }
   </pre>
 **/

public class FeatureList {

    public static final String SIZE = "SIZE";
    public static final String MDTM = "MDTM";
    public static final String PARALLEL = "PARALLEL";
    public static final String ESTO = "ESTO";
    public static final String ERET = "ERET";
    public static final String SBUF = "SBUF";
    public static final String ABUF = "ABUF";
    public static final String DCAU = "DCAU";
    public static final String PIPE = "PIPE";

    protected Vector featVector;

    public FeatureList(String featReplyMsg) {

	featVector = new Vector();

	StringTokenizer responseTokenizer
	    = new StringTokenizer(featReplyMsg, 
				  System.getProperty("line.separator"));

	// ignore the first part of the message
	if (responseTokenizer.hasMoreElements()) {
	    responseTokenizer.nextToken();
	}

	while ( responseTokenizer.hasMoreElements() ) {
	    String line = (String) responseTokenizer.nextElement();
	    line = line.trim().toUpperCase();
	    if ( !line.startsWith( "211 END" ) ) {
		featVector.add( line );
	    }
	}
    }

    public boolean contains(String feature) {
	if (feature == null) {
	    throw new IllegalArgumentException();
	}
	return featVector.contains(feature.toUpperCase());
    }    
}
