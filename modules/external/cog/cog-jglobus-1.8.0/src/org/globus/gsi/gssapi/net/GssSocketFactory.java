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
package org.globus.gsi.gssapi.net;

import java.net.Socket;
import java.io.IOException;

import org.ietf.jgss.GSSContext;

public abstract class GssSocketFactory {
  
    private static GssSocketFactory factory = null;

    public synchronized static GssSocketFactory getDefault() {
	if (factory == null) {
	    String className = System.getProperty("org.globus.gsi.gssapi.net.provider");
	    if (className == null) {
		className = "org.globus.gsi.gssapi.net.impl.GSIGssSocketFactory";
	    }
	    try {
		Class clazz = Class.forName(className);
		if (!GssSocketFactory.class.isAssignableFrom(clazz)) {
		    throw new RuntimeException("Invalid GssSocketFactory provider class");
		}
		factory = (GssSocketFactory)clazz.newInstance();
	    } catch (ClassNotFoundException e) {
		throw new RuntimeException("Unable to load '" + className + "' class: " +
					   e.getMessage());
	    } catch (InstantiationException e) {
		throw new RuntimeException("Unable to instantiate '" + className + "' class: " +
					   e.getMessage());
	    } catch (IllegalAccessException e) {
		throw new RuntimeException("Unable to instantiate '" + className + "' class: " +
					   e.getMessage());
	    }
	}
	return factory;
    }

    public abstract Socket createSocket(Socket s, 
					String host,
					int port, 
					GSSContext context);

    public abstract Socket createSocket(String host, 
					int port, 
					GSSContext context)
	throws IOException;
    
}
