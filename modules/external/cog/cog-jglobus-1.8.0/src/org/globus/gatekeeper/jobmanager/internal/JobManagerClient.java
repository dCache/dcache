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

import org.globus.gatekeeper.jobmanager.JobManagerService;
import org.globus.gatekeeper.jobmanager.JobManagerException;

import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * JobManagerClient is a Thread that has a connection to a client and will handle
 * the request that the client sends to the JobManager.
 * supported requests are:<p>
 * status<p>
 * register <callbackurl> <job-state-mask><p>
 * unregister <callbackurl><p>
 * cancel <p>
 */
public class JobManagerClient extends Thread {

    private Socket _socket = null;
    private JobManagerService _jobManager = null;
    private InputStream _input = null;
    private OutputStream _output = null;

    private JobManagerProtocol _jobManagerProtocol = null;

    /**
     * Creates an instance of the HandleClientRequest and immediately
     * starts running the Thread to handle the client's request.
     * @param socket must be connected to the client.
     * @param jm the JobManager for which the client is connected to.
     */
    public JobManagerClient(Socket socket, 
			    JobManagerService jm) {
	_socket = socket;
	_jobManager = jm;
	_jobManagerProtocol = JobManagerProtocol.getInstance("GRAM1.0");
	start();
    }

    /**
     * executes an internal method to handle the client request and response.
     */
    public void run(){
	try {
	    _input = _socket.getInputStream();
            _output = _socket.getOutputStream();
	    
	    _jobManagerProtocol.handleRequest(_jobManager, _input);
	    sendOKMessage();
	    
	} catch(JobManagerException e) {
	    sendFailureMessage(e);
	} catch(IOException e) {
	    e.printStackTrace();
	} catch(Exception e) {
	    sendFailureMessage(e);
	} finally {
	    close();
	}
    }
    
    private void close() {
        if (_output != null) {
            try { _output.close(); } catch(Exception e) {}
        }
        if (_input != null) {
            try { _input.close(); } catch(Exception e) {}
        }
        if (_socket != null) {
            try { _socket.close(); } catch(Exception e) {}
        }
    }

    private void write(String msg) {
        if (msg == null) return;
        try {
            _output.write(msg.getBytes());
        } catch(Exception e) {
        }
    }
    
    /**
     * sends an okay message to the client
     */
    private void sendOKMessage(){
	write(_jobManagerProtocol.getRequestReply(_jobManager.getStatus(),
						  _jobManager.getFailureCode()));
    }
    
    /**
     * sends a failure message to the client
     */
    private void sendFailureMessage(Exception e){
	e.printStackTrace();
	write(_jobManagerProtocol.getErrorMessage(e));
    }
    
}
