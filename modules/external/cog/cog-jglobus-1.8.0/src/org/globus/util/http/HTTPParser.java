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

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class HTTPParser {

    private static Log logger =
	LogFactory.getLog(HTTPParser.class.getName());

    protected String _server;
    protected String _host;
    protected String _contentType;
    protected String _connection;
    protected long _contentLength;
    protected boolean _chunked;
    protected LineReader _reader;
    
    public HTTPParser(InputStream is) 
	throws IOException {
	_contentLength = -1;
	_chunked = false;
	setInputStream(is);
	parse();
    }

    public String getContentType() {
	return _contentType;
    }

    public long getContentLength() {
	return _contentLength;
    }

    public boolean isChunked() {
	return _chunked;
    }
    
    public LineReader getReader() {
	return _reader;
    }
    
    public void setInputStream(InputStream in) {
	_reader = new LineReader(in);
    }
    
    public abstract void parseHead(String line) 
	throws IOException;
    
    /**
     * Parses the typical HTTP header.
     * @exception IOException if a connection fails or bad/incomplete request
     */
    protected void parse() 
	throws IOException {
	
	String line;
	line = _reader.readLine();
	if (logger.isTraceEnabled()) {
	    logger.trace(line);
	}
	parseHead(line);

	while ( (line = _reader.readLine()).length() != 0 ) {
	    if (logger.isTraceEnabled()) {
		logger.trace(line);
	    }
	    
	    if (line.startsWith(HTTPProtocol.CONNECTION)) {
		_connection = getRest(line, HTTPProtocol.CONNECTION.length());
	    } else if (line.startsWith(HTTPProtocol.SERVER)) {
		_server = getRest(line, HTTPProtocol.SERVER.length());
	    } else if (line.startsWith(HTTPProtocol.CONTENT_TYPE)) {
		_contentType = getRest(line, HTTPProtocol.CONTENT_TYPE.length());
	    } else if (line.startsWith(HTTPProtocol.CONTENT_LENGTH)) {
		_contentLength = Long.parseLong(getRest(line, 
							HTTPProtocol.CONTENT_LENGTH.length()));
	    } else if (line.startsWith(HTTPProtocol.HOST)){
		_host = getRest(line, HTTPProtocol.HOST.length());
	    } else if (line.startsWith(HTTPProtocol.CHUNKED)) {
		_chunked = true;
	    }
	    
	}
    }
    
    protected static final String getRest(String line, int index) {
	return line.substring(index).trim();
    }
    
}
