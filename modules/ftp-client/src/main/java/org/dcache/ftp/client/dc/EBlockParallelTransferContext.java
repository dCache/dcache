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
package org.dcache.ftp.client.dc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
This context has two functions. First, it keeps tracks of EODs. Second, it has the pool
of the available sockets.
 */
public class EBlockParallelTransferContext
        implements TransferContext
{

    protected static final Logger logger = LoggerFactory.getLogger(EBlockParallelTransferContext.class);

    protected SocketPool socketPool;

    protected Object quitToken = new Object();

    // since the threadmanager won't change during one transfer,
    // the context is being used as a reference holder for transferThreadManger
    private TransferThreadManager transferThreadManager;

    public static final int UNDEFINED = -1;
    /**
     * if sending data, this is interpreted as the number of EODS
     * sent. If receiving data, this is the number of EODS received.
     **/
    protected int eodsTransferred = 0;
    /**
     * if sending data, this is the total number of EODS we should send.
     * if receiving data, this is the total number of EODS we are expecting.
     **/
    protected int eodsTotal = UNDEFINED;


    public synchronized void eodTransferred()
    {
        eodsTransferred++;
    }

    public synchronized int getEodsTransferred()
    {
        return eodsTransferred;
    }

    public synchronized void setEodsTotal(int total)
    {
        eodsTotal = total;
    }

    public synchronized int getEodsTotal()
    {
        return eodsTotal;
    }

    /**
     * release the token if and only if (all EODS have been sent, or all EODS have been
     * received), and the token has not been released yet.
     * So this method will return non-null only one in the instance's lifetime.
     **/
    @Override
    public synchronized Object getQuitToken()
    {
        logger.debug("checking if ready to quit");
        logger.debug("eodsTotal = {}; eodsTransferred = {}", eodsTotal, eodsTransferred);
        if (eodsTotal != UNDEFINED &&
            eodsTransferred == eodsTotal) {
            // ready to release the quit token. But make sure not to do it twice.
            // This section only returns non-nul the first time it is entered.
            Object myToken = quitToken;
            quitToken = null;
            return myToken;
        } else {
            // not ready to quit yet
            return null;
        }
    }

    public synchronized void setSocketPool(SocketPool sp)
    {
        this.socketPool = sp;
    }

    public synchronized SocketPool getSocketPool()
    {
        return this.socketPool;
    }

    public void setTransferThreadManager(TransferThreadManager transferThreadManager)
    {
        this.transferThreadManager = transferThreadManager;
    }

    public TransferThreadManager getTransferThreadManager()
    {
        return transferThreadManager;
    }


}

