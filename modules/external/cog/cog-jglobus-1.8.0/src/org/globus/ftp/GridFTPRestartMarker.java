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
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Represents GridFTP restart marker, which contains unordered set 
   of byte ranges representing transferred data. 
   The ranges are preserved exactly as received 
   from the server, which may not be very useful. 
   For additional processing on byte ranges, 
   use ByteRangeList.
   Typical usage:
   <pre>
   list = new ByteRangeList();
   marker = new GridFTPRestartMarker(reply.getMessage());
   list.merge(marker.getVector());
   </pre>
   @see ByteRangeList
 **/
public class GridFTPRestartMarker implements Marker {

    private static Log logger = 
        LogFactory.getLog(GridFTPRestartMarker.class.getName());

    Vector vector;


    /**
       Constructs the restart marker by parsing the parameter string.
       @param msg The string in the format of FTP reply 111 message,
       for instance "Range Marker 0-29,30-89"
       @throws IllegalArgumentException if the parameter is in bad format
     **/
    public GridFTPRestartMarker(String msg)
	throws IllegalArgumentException{

	// expecting msg like "Range Marker 0-29,30-89"

	vector = new Vector();
	StringTokenizer tokens = new StringTokenizer(msg);
	
	if (! tokens.hasMoreTokens()) {
	    badMsg("message empty", msg);
	}

	if (! tokens.nextToken(" ").equals("Range")) {
	    badMsg("should start with Range Marker", msg);
	}
	if (! tokens.nextToken(" ").equals("Marker")) {
	    badMsg("should start with Range Marker", msg);
	}

	while(tokens.hasMoreTokens()) {
	    long from =0;
	    long to =0;
	    try {

		String rangeStr = tokens.nextToken(",");
		StringTokenizer rangeTok = new StringTokenizer(rangeStr, "-");
		from = Long.parseLong(rangeTok.nextToken().trim());
		to = Long.parseLong(rangeTok.nextToken().trim());

		if (rangeTok.hasMoreElements()) {
		    badMsg("A range is followed by '-'", msg);
		}

	    } catch(NoSuchElementException nse) {
		// range does not look like "from-to"
		badMsg("one of the ranges is malformatted", msg);
	    } catch (NumberFormatException nfe) {
		badMsg("one of the integers is malformatted", msg);
	    }

	    try {

		vector.add(new ByteRange(from, to));

	    } catch(IllegalArgumentException iae) {
		// to < from
		badMsg("range beginning > range end", msg);
	    }   
	}
	//vector now contains all ranges
	if (vector.size() == 0 ) {
	    badMsg("empty range list", msg);
	}
    };

    private void badMsg(String why, String msg) {
	throw new IllegalArgumentException(
	    "argument is not FTP 111 reply message ("
	    + why + ": ->" + msg + "<-");
    }

    /**
      Returns Vector representation of this object.  Its elements
      are be ByteRange objects. They are in the order exactly as received
      in the FTP reply; no additional processing has been done on them.
      To order and merge them, use ByteRangeList.
      Subsequent calls of this method will return
      the same Vector object.
      @return Vector representation of this object.
    **/
    public Vector toVector() {
	return vector;
    };
    
}
