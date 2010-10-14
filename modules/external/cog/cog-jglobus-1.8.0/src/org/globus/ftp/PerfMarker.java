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

import org.globus.ftp.exception.PerfMarkerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.StringTokenizer;

/**
   Represents GridFTP performance marker.
   Use getter methods to access its parameters.
 **/
public class PerfMarker implements Marker {
    
    private static Log logger = LogFactory.getLog(PerfMarker.class.getName());

    protected final String nl = System.getProperty("line.separator");

    protected final static int UNDEFINED = -1;
    // must have timeStamp
    protected boolean hasTimeStamp = false;
    protected double timeStamp = UNDEFINED;

    protected boolean hasStripeIndex = false;
    protected long stripeIndex = UNDEFINED;

    protected boolean hasStripeBytesTransferred = false;
    protected long stripeBytesTransferred = UNDEFINED;

    protected boolean hasTotalStripeCount = false;
    protected long totalStripeCount = UNDEFINED;

    /**
       @param msg an FTP reply message containing the perf marker (not the reply itself!)
     **/
    public PerfMarker(String msg) 
    throws IllegalArgumentException{
	StringTokenizer tokens = new StringTokenizer(msg, nl);
	if (! tokens.nextToken().trim().equals("Perf Marker")) {
	    badMsg("should start with Perf Marker'", msg);
	}
	if (! tokens.hasMoreTokens()) {
	    badMsg("No parameters", msg);
	}
	
	//traverse lines
	while(tokens.hasMoreTokens()) {

	    //line = "name : value"
	    StringTokenizer line = new StringTokenizer(tokens.nextToken(), 
						       ":");
	    if (! line.hasMoreTokens()) {
		badMsg("one of lines empty", msg);
	    }

	    // name
	    String name = line.nextToken();

	    if (! name.startsWith(" ")) {
		//last line
		if (! name.startsWith("112")) {
		    //that wasn't a 112 message!
		    logger.debug("ending line: ->" + name +"<-");
		    badMsg("No ending '112' line", msg);
		}
		break;
	    }

	    name = name.trim();

	    if (! line.hasMoreTokens()) {
		badMsg("one of parameters has no value", msg);
	    }

	    // value
	    String value = line.nextToken().trim();

	    if(name.equals( "Timestamp")) {

		try {
		    timeStamp = Double.parseDouble(value);
		    hasTimeStamp = true;
		} catch ( NumberFormatException e) {
		    badMsg("Not double value:" + value, msg);
		}

	    } else if (name.equals("Stripe Index")) {

		try {
		    stripeIndex = Long.parseLong(value);
		    hasStripeIndex = true;
		} catch ( NumberFormatException e) {
		    badMsg("Not long value:" + value, msg);
		}
		
	    } else if (name.equals("Stripe Bytes Transferred")) {

		try {
		    stripeBytesTransferred = Long.parseLong(value);
		    hasStripeBytesTransferred = true;
		} catch ( NumberFormatException e) {
		    badMsg("Not long value:" + value, msg);
		}

	    } else if (name.equals("Total Stripe Count")) {

		try {
		    totalStripeCount = Long.parseLong(value);
		    hasTotalStripeCount = true;
		} catch ( NumberFormatException e) {
		    badMsg("Not long value:" + value, msg);
		}

	    }

	}//traverse lines

	//marker must contain time stamp
	if (!hasTimeStamp) {
	    badMsg("no timestamp", msg);
	}
    }//PerfMarker

    private void badMsg(String why, String msg) {
	throw new IllegalArgumentException(
	    "argument is not FTP 112 reply message ("
	    + why + ": ->" + msg + "<-");
    }

    public boolean hasStripeIndex() {
	return hasStripeIndex;
    }

    public boolean hasStripeBytesTransferred() {
	return hasStripeBytesTransferred;
    }

    public boolean hasTotalStripeCount() {
	return hasTotalStripeCount;
    }

    public double getTimeStamp() {
	return timeStamp;
    }

    public long getStripeIndex()
	throws PerfMarkerException {
	if (! hasStripeIndex) {
	    throw new PerfMarkerException(
				      PerfMarkerException.NO_SUCH_PARAMETER);
	}
	return stripeIndex;
    }

    public long getStripeBytesTransferred()
	throws PerfMarkerException {
	if (! hasStripeBytesTransferred) {
	    throw new PerfMarkerException(
				      PerfMarkerException.NO_SUCH_PARAMETER);
	}
	return stripeBytesTransferred;
    }

    public long getTotalStripeCount()
	throws PerfMarkerException {
	if (! hasTotalStripeCount) {
	    throw new PerfMarkerException(
				      PerfMarkerException.NO_SUCH_PARAMETER);
	}
	return totalStripeCount;
    }

}
