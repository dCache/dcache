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
package org.globus.gatekeeper.jobmanager.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;

import org.apache.log4j.Logger;

// FIXME: maybe this should read the input stream line by line?
public class Pipe implements Runnable {

    private static final int BUFFER_SIZE = 1024;

    private boolean _stop = false;
    private InputStream _in;
    private OutputStream _out;

    private Logger _logger;

    public Pipe() {
    }

    public void setLogger(Logger logger) {
        _logger = logger;
    }

    public void setInputStream(InputStream in) {
	_in = in;
    }
    
    public void setOutputStream(OutputStream out) {
	_out = out;
    }

    public void start() {
	Thread t = new Thread(this);
	t.start();
    }

    public void stop() {
	_stop = true;
    }

    public void run() {
	_stop = false;

	_logger.info("[pipe] running...");

	byte [] buffer = new byte[BUFFER_SIZE];
	
	int bytes = 0;
	try {
	    while( (bytes = _in.read(buffer)) != -1) {
		_out.write(buffer, 0, bytes);
		// This causes 'Bad descriptor error' from time to time.
		//_out.flush();  
		if (_stop) break;
	    }
	} catch(IOException e) {
	    _logger.debug("Unexpected error.", e);
	} finally {
	    try { _out.close(); } catch(Exception e) {}
	    try { _in.close(); } catch(Exception e) {}
	}

	_logger.info("[pipe] done.");
    }
    
}
