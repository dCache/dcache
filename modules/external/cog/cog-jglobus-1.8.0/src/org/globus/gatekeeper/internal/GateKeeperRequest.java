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
package org.globus.gatekeeper.internal;

import java.io.InputStream;
import java.io.IOException;

import org.globus.gatekeeper.ServiceRequest;
import org.globus.util.http.HTTPRequestParser;

public class GateKeeperRequest extends HTTPRequestParser implements ServiceRequest {
    
    private static final String PING = "ping";

    public GateKeeperRequest(InputStream in)
	throws IOException {
	super(in);
    }

    public InputStream getInputStream() {
	return getReader().getInputStream();
    }
    
    public boolean isPing() {
	return (_service != null && _service.regionMatches(true, 0, PING, 0, 4));
    }
    
    public String getService() {
	if (_service == null) return null;
	
	if (_service.charAt(0) == '/') {
	    return _service.substring(1);
	} else if (_service.regionMatches(true, 0, PING, 0, 4)) {
	    return _service.substring(5);
	} else {
	    return _service;
	}
    }
    
}
