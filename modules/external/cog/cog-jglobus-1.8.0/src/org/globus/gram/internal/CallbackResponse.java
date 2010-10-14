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
package org.globus.gram.internal;

import java.io.InputStream;
import java.io.IOException;

public class CallbackResponse extends GatekeeperReply {
    
    protected String httpMethod  = null;
    protected String callbackURL = null;
    
    public CallbackResponse(InputStream in) throws IOException {
	super(in);
    }

    public void parseHttp(String line) {
	
	int p1 = line.indexOf(" ");
	if (p1 == -1) {
	    return;
	}

	httpMethod = line.substring(0, p1);

	int p2 = line.indexOf(" ", p1+1);
	if (p2 == -1) {
	    return;
	}

	callbackURL = line.substring(p1+1, p2);
	
	int p3 = line.indexOf(" ", p2+1);
	if (p3 == -1) {
	    return;
	}
	
	httpType    = line.substring(p2+1);
    }
    
    public String toString() {
	StringBuffer buf = new StringBuffer();
	
	buf.append("HttpMethod : " + httpMethod + "\n");
	buf.append("URL        : " + callbackURL + "\n");
	buf.append(super.toString());
	
	return buf.toString();
    }
}
	


