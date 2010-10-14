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

import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

public class TestUtil {

    private Properties props;

    public TestUtil(String config) throws Exception {
	Thread t = Thread.currentThread();
	InputStream in = null;
	try {
	    in = t.getContextClassLoader().getResourceAsStream(config);
	    
	    if (in == null) {
		throw new IOException("Test configuration file not found: " +
				      config);
	    }

	    props = new Properties();
	    props.load(in);
	} finally {
	    if (in != null) {
		in.close();
	    }
	}
    }

    public String get(String propName) {
	return props.getProperty(propName);
    }

    public int getAsInt(String propName) {
	String value = props.getProperty(propName);
	return Integer.parseInt(value);
    }
    
}
