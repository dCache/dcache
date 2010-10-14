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
package org.globus.mds.gsi.common;

import java.io.InputStream;
import java.io.IOException;

import org.globus.gsi.gssapi.SSLUtil;
import org.globus.gsi.gssapi.net.GssInputStream;

import org.ietf.jgss.GSSContext;

public class SaslInputStream extends GssInputStream {
  
    private byte [] lenBuf;

    public SaslInputStream(InputStream in, GSSContext context) {
	super(in, context);
	this.lenBuf = new byte[4];
    }
  
    protected void readMsg()
	throws IOException {
	SSLUtil.readFully(this.in, this.lenBuf, 0, this.lenBuf.length);
        int len = GSIMechanism.networkByteOrderToInt(this.lenBuf, 0, 4);
	byte [] msg = new byte[len];
	SSLUtil.readFully(this.in, msg, 0, len);
	this.buff = msg;
	this.index = 0;
    }
    
}
