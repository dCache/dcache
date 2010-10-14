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
package org.globus.gsi.gssapi.net.impl;

import java.io.OutputStream;
import java.io.IOException;

import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssOutputStream;
import org.globus.gsi.gssapi.SSLUtil;

import org.ietf.jgss.GSSContext;

public class GSIGssOutputStream extends GssOutputStream {

    protected byte [] header;
    protected int mode;
    
    public GSIGssOutputStream(OutputStream out, GSSContext context) {
	this(out, context, GssSocket.SSL_MODE);
    }

    public GSIGssOutputStream(OutputStream out, GSSContext context, int mode) {
	super(out, context);
	this.header = new byte[4];
	setWrapMode(mode);
    }

    public void flush() 
	throws IOException {
	if (this.index == 0) return;
	writeToken(wrap());
	this.index = 0;
    }

    public void setWrapMode(int mode) {
	this.mode = mode;
    }

    public int getWrapMode() {
	return this.mode;
    }

    public void writeToken(byte[] token)
	throws IOException {
	if (this.mode == GssSocket.GSI_MODE) {
	    SSLUtil.writeInt(token.length, this.header, 0);
	    this.out.write(this.header);
	}
	this.out.write(token);
	this.out.flush();
    }
    
}
