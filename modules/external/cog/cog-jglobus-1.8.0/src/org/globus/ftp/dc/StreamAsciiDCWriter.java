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
package org.globus.ftp.dc;

import java.io.IOException;

import org.globus.ftp.Buffer;

public class StreamAsciiDCWriter extends StreamImageDCWriter {

    protected AsciiTranslator translator;

    public StreamAsciiDCWriter() {
	// check for \r\n and \n separators 
	// output tokens with \r\n line separators
	translator = new AsciiTranslator(true, true, AsciiTranslator.CRLF);
    }

    public void write(Buffer buffer) 
	throws IOException {
	if (buffer == null) return;
	super.write( translator.translate(buffer) );
    }
    
}
