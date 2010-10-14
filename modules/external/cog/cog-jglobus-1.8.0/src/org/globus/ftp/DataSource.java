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
 * Data channel uses this interface to read outgoing data.
 * Implement it to provide your own ways of reading data.
 * It must be thread safe; in parallel transfer mode several
 * streams may attempt to read.
 **/
public interface DataSource {

    /**
     * Reads a data buffer from this data source.
     *
     * @return The data buffer read. Null, if there is
     *         no more data to be read.
     * @throws IOException if an I/O error occurs.
     */
    public Buffer read()
       throws IOException;

    /**
     * Closes this data source and releases any system 
     * resources associated with this source.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void close()
	throws IOException;

    /**
     * Optional operation. Returns the total size, in bytes, of the
     * data in this source. If the implementation is not able to 
     * provide a total size for the data source, it should return
     * -1
     * 
     * @throws IOException if an I/O exception occurs
     */
    public long totalSize() throws IOException;
}
