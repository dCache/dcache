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
package org.globus.util.http;

import java.io.InputStream;
import java.io.IOException;

public class HTTPRequestParser extends HTTPParser {

    protected String _requestType;
    protected String _service;
    
    public HTTPRequestParser(InputStream is) 
	throws IOException {
	super(is);
    }

    public String getService() {
	return _service;
    }
    
    public void setService(String service) {
	_service = service;
    }

    public void parseHead(String line) 
	throws IOException {
	int st = line.indexOf(" ");
	if (st == -1) {
	    throw new IOException("Bad HTTP header");
	}
	_requestType = line.substring(0, st);
	
	st++;
	int et = line.indexOf(" ", st);
	if (et == -1) {
	    throw new IOException("Bad HTTP header");
	}
	_service = line.substring(st, et);
	
	et++;
    }
    
}
