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

/**
   Represents a chunk of data cut out of a larger data volume.
   Buffer is characterized by offset at which it belongs to the
   larger data volume, and length.
   The internal data array always starts at 0 and ends at (length -1).
   Its indexing has nothing to do with offset.
 **/
public class Buffer {

    protected byte[] buf;
    protected int length;
    protected long offset;

    /**
     * @param buf the data buffer (always starts at 0)
     * @param length length of the data in the buffer
     */
    public Buffer(byte [] buf, int length) {
	this(buf, length, -1);
    }

    /**
     * @param buf the data buffer (always starts at 0)
     * @param length length of the data in the buffer
     * @param offset offset of the data the buffer was read from.
     *              
     */
    public Buffer(byte [] buf, int length, long offset) {
	this.buf = buf;
	this.length = length;
	this.offset = offset;
    }

    public byte[] getBuffer() {
	return buf;
    }

    public int getLength() {
	return length;
    }

    /**
     * Returns offset of the data the buffer was read from.
     * Value -1 indicates that offset is not supported.
     * For instance, this will happen if the buffer represents
     * a chunk of data read off the data channel in the stream
     * mode. 
     *
     */
    public long getOffset() {
	return offset;
    }

}
