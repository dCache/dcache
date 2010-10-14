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

public class StreamAsciiDCReader extends StreamImageDCReader {

    protected AsciiTranslator translator;

    public StreamAsciiDCReader() {
	// only check for \r\n separators - others are ignored
	// output tokens with system specific line separators
	translator = new AsciiTranslator(true, false);
    }

    public Buffer read()
	throws IOException {
	Buffer buf = super.read();
	if (buf == null) {
	    return null;
	}
	return translator.translate(buf);
    }

}
