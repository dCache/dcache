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
package org.globus.gatekeeper;

import org.ietf.jgss.GSSCredential;

/**
 * Provides a simple interface is accessed by the
 * gatekeeper to forward service requests and handle
 * service-specific protocol replies.
 */
public interface Service {

    /**
     * Sets the credentials for the service.
     */
    public void setCredentials(GSSCredential cred);

    /**
     * Invokes the service with given request.
     */
    public void request(ServiceRequest request) 
	throws ServiceException;
    
    /**
     * Retrieves a handle to this service.
     */
    public String getHandle();
    
    public void setArguments(String [] args) 
	throws ServiceException;
    
    // these are for protocol specific stuff
    
    public String getRequestSuccessMessage();
    
    public String getRequestFailMessage(Exception e);
    
}
