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
package org.globus.gatekeeper.jobmanager;

import org.globus.gatekeeper.ServiceException;

public class ShellJobManagerService extends JobManagerService {

    public ShellJobManagerService() {
        super( new ShellJobManager() );
    }
    
    public void setArguments(String [] args)
        throws ServiceException {
	
	ShellJobManager jobManager = (ShellJobManager)_jobManager;
	
	for (int i=0;i<args.length;i++) {
	    
	    if (args[i].equalsIgnoreCase("-type")) {
		jobManager.setType( args[++i] );
	    } else if (args[i].equalsIgnoreCase("-libexec")) {
		jobManager.setLibExecDirectory( args[++i] );
	    }
	    
	}
    }

}


