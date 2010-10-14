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
   Represents a data channel reader or writer
   aware of being in one of a pool of asynchronous
   data channels
 **/
public class EBlockAware {

    public static final int 
	EOF = 64,
	EOD = 8,
	WILL_CLOSE = 4;
    
    protected EBlockParallelTransferContext context;
    

    public void setTransferContext(EBlockParallelTransferContext context) {
	this.context = context;
    }

}
