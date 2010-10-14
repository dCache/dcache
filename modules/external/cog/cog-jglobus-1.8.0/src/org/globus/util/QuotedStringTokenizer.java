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

import java.util.Enumeration;

/*
 * This class is not thread safe.
 */
public class QuotedStringTokenizer implements Enumeration {
    
    private int limit;
    private int start;
    private String str;

    public QuotedStringTokenizer(String str) {
	this.str = str;
	start = 0;
	limit = str.length();
    }

    public Object nextElement() {
	return nextToken();
    }

    public String nextToken() {
	while ((start < limit) && Character.isWhitespace(str.charAt(start))) {
	    start++;	// eliminate leading whitespace
	}

	if (start == limit) return null;

	StringBuffer buf = new StringBuffer(limit-start);
	char ch;
	char quote = str.charAt(start);
	if (quote == '"' || quote == '\'') {
	    start++;
	    for (int i=start;i<limit;i++) {
		ch = str.charAt(i);
		start++;
		if (ch == quote) {
		    break;
		} else if (ch == '\\') {
		    buf.append( str.charAt(++i) );
		    start++;
		} else {
		    buf.append(ch);
		}
	    }
	    return buf.toString();
	} else {
	    for (int i=start;i<limit;i++) {
		ch = str.charAt(i);
		start++;
		if (Character.isWhitespace(ch)) {
		    break;
		} else {
		    buf.append(ch);
		}
	    }
	}
	
	return buf.toString();
    }

    public boolean hasMoreElements() {
	return hasMoreTokens();
    }

    public boolean hasMoreTokens() {
	while ((start < limit) && (str.charAt(start) <= ' ')) {
	    start++;	// eliminate leading whitespace
	}
	
	return (start != limit);
    }

    public int countTokens() {
	int localStart = start;
	int i = 0;
	while( nextToken() != null ) {
	    i++; 
	}
	start = localStart;
	return i;
    }
    
}
