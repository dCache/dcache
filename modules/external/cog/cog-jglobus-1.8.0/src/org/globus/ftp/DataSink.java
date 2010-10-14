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
package org.globus.ftp;

import java.io.IOException;

/**
 * Data channel uses this interface to write the incoming data.
 * Implement it to provide your own ways of storing data.
 * It must be thread safe; in parallel transfer mode several
 * streams may attempt to write.
 **/
public interface DataSink {
    
    /**
     * Writes the specified buffer to this data sink. <BR>
     * <i>Note: {@link Buffer#getOffset() buffer.getOffset()} might
     * return -1 if the transfer mode used does not support
     * data offsets, for example stream transfer mode.</i>
     *
     * @param buffer the data buffer to write. 
     * @throws IOException if an I/O error occurs.
     */
    public void write(Buffer buffer)
	throws IOException;
    
    /**
     * Closes this data sink and releases any system 
     * resources associated with this sink.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void close()
	throws IOException;
    
}
