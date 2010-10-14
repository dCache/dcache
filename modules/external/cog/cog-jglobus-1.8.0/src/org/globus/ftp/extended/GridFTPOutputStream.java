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
package org.globus.ftp.extended;

import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

import org.globus.gsi.gssapi.net.GssOutputStream;
import org.globus.util.Base64;

import org.ietf.jgss.GSSContext;

public class GridFTPOutputStream extends GssOutputStream {
    
    private static final byte[] CRLF = "\r\n".getBytes();
    private static final byte[] ADAT = "ADAT ".getBytes();
    private static final byte[] MIC  = "MIC ".getBytes();
    private static final byte[] ENC  = "ENC ".getBytes();
    
    public GridFTPOutputStream(OutputStream out, GSSContext context) {
	super(new BufferedOutputStream(out), context);
    }
    
    public void flush() 
	throws IOException {
	if (this.index == 0) return;
	if (this.context.getConfState()) {
	    writeToken(ENC, wrap());
	} else {
	    writeToken(MIC, wrap());
	}
	this.index = 0;
    }
    
    public void writeHandshakeToken(byte [] token) 
	throws IOException {
	writeToken(ADAT, token);
    }

    private void writeToken(byte[] header, byte[] token)
        throws IOException {
        this.out.write(header);
        this.out.write(Base64.encode(token));
        this.out.write(CRLF);
        this.out.flush();
    }

}
