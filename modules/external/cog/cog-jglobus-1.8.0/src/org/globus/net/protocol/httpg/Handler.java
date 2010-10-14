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
package org.globus.net.protocol.httpg;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.lang.reflect.Constructor;

public class Handler extends URLStreamHandler {

    private static final String CLASS =
        "org.globus.net.GSIHttpURLConnection";

    private static final Class[] PARAMS = 
        new Class[] { URL.class };
        
    private static Constructor constructor = null;

    private static synchronized Constructor initConstructor() {
        if (constructor == null) {
            ClassLoader loader = 
                Thread.currentThread().getContextClassLoader();
            try {
                Class clazz = Class.forName(CLASS, true, loader);
                constructor = clazz.getConstructor(PARAMS);
            } catch (Exception e) {
                throw new RuntimeException("Unable to load url handler: " +
                                           e.getMessage());
            }
        }
        return constructor;
    }
    
    protected URLConnection openConnection(URL u) {
        if (constructor == null) {
            initConstructor();
        }
        try {
            return (URLConnection)constructor.newInstance(new Object[] {u});
        } catch (Exception e) {
            throw new RuntimeException("Unable to instantiate url handler: " +
                                       e.getMessage());
        }
    }
    
    protected int getDefaultPort() {
	return 8443;
    }

    protected void setURL(URL u, String protocol, String host, int port,
			  String authority, String userInfo, String path,
			  String query, String ref) {
	if (port == -1) {
	    port = getDefaultPort();
	}
	super.setURL(u, protocol, host, port, authority, userInfo,
		     path, query, ref);
    }

}
