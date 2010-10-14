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

import java.io.OutputStream;
import java.io.IOException;

import org.globus.gsi.gssapi.net.GssOutputStream;

import org.ietf.jgss.GSSContext;

public class SaslOutputStream extends GssOutputStream {
    
    private byte [] lenBuf;

    public SaslOutputStream(OutputStream out, GSSContext context) {
	super(out, context);
	this.lenBuf = new byte[4];
    }

    public void flush() 
	throws IOException {
	if (index == 0) return;
	byte [] token = wrap();
	GSIMechanism.intToNetworkByteOrder(token.length, lenBuf, 0, 4);
	this.out.write(lenBuf);
	this.out.write(token);
	this.out.flush();
	index = 0;
    }
    
}
