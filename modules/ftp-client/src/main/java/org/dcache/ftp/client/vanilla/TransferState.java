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
package org.dcache.ftp.client.vanilla;

import java.io.InterruptedIOException;
import java.io.IOException;

import org.dcache.ftp.client.exception.ClientException;
import org.dcache.ftp.client.exception.ServerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferState
{

    private static final Logger logger =
            LoggerFactory.getLogger(TransferState.class);

    private int transferDone;
    private int transferStarted;
    private Exception transferException = null;

    public TransferState()
    {
        this.transferDone = 0;
        this.transferStarted = 0;
        this.transferException = null;
    }

    // this is called when transfer successfully started (opening data conn)
    public synchronized void transferStarted()
    {
        this.transferStarted++;
        notifyAll();
    }

    // this is called when TransferMonitor thread is finished
    public synchronized void transferDone()
    {
        this.transferDone++;
        notifyAll();
    }

    // this is called when an error occurs during transfer
    public synchronized void transferError(Exception e)
    {
        logger.debug("intercepted exception", e);
        if (transferException == null) {
            transferException = e;
        } else if (transferException instanceof InterruptedException
                   || transferException instanceof InterruptedIOException) {
            //if one of the threads throws an error, it interrupts
            //the other thread (by InterruptedException).
            //Here we make sure that transferException will store the
            //primary failure reason, not the resulting InterruptedException
            transferException = e;
        }
        notifyAll();
    }

    public synchronized boolean isDone()
    {
        return this.transferDone >= 2;
    }

    public synchronized boolean isStarted()
    {
        return this.transferStarted >= 2;
    }

    /**
     * Blocks until the transfer is complete or
     * the transfer fails.
     */
    public synchronized void waitForEnd()
            throws ServerException,
            ClientException,
            IOException
    {
        try {
            while (!isDone() && !hasError()) {
                wait();
            }
        } catch (InterruptedException e) {
            // break
        }
        checkError();
    }

    /**
     * Blocks until the transfer begins or
     * the transfer fails to start.
     */
    public synchronized void waitForStart()
            throws ServerException,
            ClientException,
            IOException
    {
        try {
            while (!isStarted() && !hasError()) {
                wait();
            }
        } catch (InterruptedException e) {
            // break
        }
        checkError();
    }

    public synchronized boolean hasError()
    {
        return (transferException != null);
    }

    public Exception getError()
    {
        return transferException;
    }

    public void checkError()
            throws ServerException,
            ClientException,
            IOException
    {
        if (transferException == null) {
            return;
        }
        if (transferException instanceof ServerException) {
            throw (ServerException) transferException;
        } else if (transferException instanceof IOException) {
            throw (IOException) transferException;
        } else if (transferException instanceof InterruptedException) {
            throw new ClientException(ClientException.THREAD_KILLED);
        }
    }

}
