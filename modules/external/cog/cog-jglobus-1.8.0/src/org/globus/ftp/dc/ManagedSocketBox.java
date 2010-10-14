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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   A container for Socket, making it available to pass a null socket reference.

   We use asynchronously active connect task to initialize socket,
   and active start transfer task to run it.
   We need to pass the socket reference, which is sometimes null
   (before initialization).
   This is a sane way to do it; a simple socket container.

   Additionally, the box contains a flag that states whether the socket 
   is currently in use, ie whether it is assigned to some data channel.
   It is needed in GridFTP for data channel reuse.
 **/

public class ManagedSocketBox extends SimpleSocketBox {

    private static Log logger = 
        LogFactory.getLog(ManagedSocketBox.class.getName());

    public static final int FREE = 1;
    public static final int BUSY = 2;
    
    public static final boolean REUSABLE = true;
    public static final boolean NON_REUSABLE = false;

    protected int status = FREE;
        
    // should the socket be reused? by default, yes
    protected boolean reusable = true;

    public ManagedSocketBox() {
    }

    public void setStatus(int s) {
        this.status = s;
    }
    
    public int getStatus() {
        return status;
    }
    
    public void setReusable(boolean r) {
        this.reusable = r;
    }
    
    public boolean isReusable() {
        return reusable;
    }   
    
}
