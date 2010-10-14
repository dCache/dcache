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
package org.globus.ftp.dc;

/**
   transfer context for single threaded transfers
   using 1 data channel.
 **/
public class SimpleTransferContext 
    implements TransferContext {


    private static SimpleTransferContext singleton = new SimpleTransferContext();

    /**
       return the default instance of this class
     **/
    public static TransferContext getDefault() {
	return singleton;
    }

    /**
       @return always non-null
     **/
    public Object getQuitToken() {
	return new Object();
    }


}
