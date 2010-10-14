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

import java.io.OutputStream;
import java.io.IOException;

import org.globus.ftp.Buffer;

public interface DataChannelWriter {

    public void setDataStream(OutputStream out);

    // looks like DataSink interface

    public void write(Buffer buffer)
	throws IOException;

    /*
      Send the mode-specific signal indicating that the data
      sending is done, but the underlying resources (the socket)
      will not necessarily be closed.
      E.g. in stream mode, do nothing. In Eblock mode, send EOD|EOF.
     */
    public void endOfData()
        throws IOException;

    /*
      Close the underlying resources (the socket)
     */
    public void close()
	throws IOException;
    
}
