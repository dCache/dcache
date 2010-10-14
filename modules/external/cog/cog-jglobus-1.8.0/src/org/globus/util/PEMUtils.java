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
package org.globus.util;

import java.io.OutputStream;
import java.io.IOException;

public class PEMUtils {
    
    static final int LINE_LENGTH = 64;
    
    public static String lineSep;
    public static byte[] lineSepBytes;
    
    static {
	lineSep = System.getProperty("line.separator");
	lineSepBytes = lineSep.getBytes();
    }
    
    public static void writeBase64(OutputStream out,
				   String header, 
				   byte[] base64Data,
				   String footer) 
	throws IOException {
	
	int length = LINE_LENGTH;
	int offset = 0;
	
	if (header != null) {
	    out.write(header.getBytes());
	    out.write(lineSepBytes);
	}
	
	int size = base64Data.length;
	while (offset < size) {
	    if (LINE_LENGTH > (size - offset)) {
		length = size - offset;
	    }
	    out.write(base64Data, offset, length);
	    out.write(lineSepBytes);
	    offset = offset + LINE_LENGTH;
	}
	
	if (footer != null) {
	    out.write(footer.getBytes());
	    out.write(lineSepBytes);
	}
    }

    /**
     * Return a hexadecimal representation of a byte array
     * @param b a byte array
     * @return String containing the hexadecimal representation
     */
    public final static String toHex(byte [] b) {
	char[] buf = new char[b.length * 2];
	int i, j, k;
	
	i = j = 0;    
	for (; i < b.length; i++) {
	    k = b[i];
	    buf[j++] = hex[(k >>> 4) & 0x0F];
	    buf[j++] = hex[ k & 0x0F];
	}
	return new String(buf);
    }
    
    private static final char[] hex = {'0','1','2','3','4','5','6','7','8','9',
				       'A','B','C','D','E','F'};

}
