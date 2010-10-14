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
package org.globus.ftp.vanilla;

import org.globus.ftp.exception.FTPReplyParseException;
import java.io.IOException;
import org.globus.ftp.exception.ServerException;

/**
   Basic subset of client side control channel functionality, enough to
   implement the part of transfer after sending transfer command (RETR)
   up until receiving 200 reply.
 **/
public abstract class BasicClientControlChannel{

    public static final int WAIT_FOREVER = -1;

    public abstract Reply read() 
        throws ServerException, 
               IOException,
               FTPReplyParseException;

    /**
       Return when reply is waiting
     **/
    public void waitFor(Flag flag,
                        int waitDelay) 
        throws ServerException,
               IOException,
               InterruptedException {
        waitFor(flag, waitDelay, WAIT_FOREVER);
    }

    /**
       Block until reply is waiting in the control channel,
       or after timeout (maxWait), or when flag changes to true.
       If maxWait == WAIT_FOREVER, do not timeout.
       @param maxWait timeout in miliseconds
     **/

    public abstract void waitFor(Flag flag,
                                 int waitDelay,
                                 int maxWait)
        throws ServerException,
               IOException,
               InterruptedException;

    /*    public void write(Command cmd)
        throws IOException,
               IllegalArgumentException;
    */

    public abstract void abortTransfer();

} //FTPServerFacade



