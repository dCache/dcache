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
package org.globus.gatekeeper.jobmanager.internal;

import java.net.Socket;
import java.io.IOException;

import org.globus.net.BaseServer;
import org.globus.gatekeeper.jobmanager.JobManagerService;

import org.ietf.jgss.GSSCredential;

 /**
  * JobManagerServer extends the BaseServer abstract class to run as a server
  * listening for clients who will request information from the JobManager or
  * invoke actions for the JobManager.
  */
public class JobManagerServer extends BaseServer {

    protected JobManagerService _jobmanager = null;

    /**
     * initializes and starts the JobManagerServer with default credentials
     */
    public JobManagerServer() 
	throws IOException{
	super();
    }
    
    /**
     * initializes and starts the JobManagerServer
     * @param cred the credentials used by this server to authenticate itself with
     * clients
     */
    public JobManagerServer(GSSCredential cred) 
	throws IOException{
	super(cred, 0);
    }
    
    /**
     * initializes and starts the JobManagerServer
     * @param cred the credentials used by this server to authenticate itself with
     * clients
     * @param port
     */
    public JobManagerServer(GSSCredential cred, int port) 
	throws IOException {
	super(cred, port);
    }
    
    /**
     * sets the JobManager which will be used by this server
     * @param jm the jobmanager that will be used to request information or
     * invoke actions
     */
    public void setJobManager(JobManagerService jm){
	_jobmanager = jm;
    }
    
    /**
     * Sets the corresponding credentials for the server in order to verify
     * that it is serving the specific JobManager (proof of identity).
     * @param cred the credentials which will be used by this Server must be
     * the same as the JobManager
     */
    public void setCredentials(GSSCredential cred){
	this.credentials = cred;
    }
    
    /**
     * Method called after a connection has been established between the
     * client and the server. Handles the client request.
     * @param socket a connected socket to the client
     */
    protected void handleConnection(Socket socket) {
	new JobManagerClient(socket, 
			     _jobmanager);
    }
}
