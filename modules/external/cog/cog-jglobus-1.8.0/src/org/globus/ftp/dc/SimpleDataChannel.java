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

import org.globus.ftp.Session;
import org.globus.ftp.DataSink;
import org.globus.ftp.DataSource;
import org.globus.ftp.vanilla.BasicServerControlChannel;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Data channel receives in the constructor a socket that should
   be ready for communication, and starts a new thread that
   will perform the transfer.<br>
   In previous version, the data channel would perform socket
   initialization (server.accept() etc.). This is now done by the
   facade's manager thread, so it can start several data channels.
 **/
public class SimpleDataChannel extends AbstractDataChannel {

    protected static Log logger =
        LogFactory.getLog(SimpleDataChannel.class.getName());

    protected SocketBox socketBox;
    protected TransferThread transferThread;
    protected TransferThreadFactory transferThreadFactory;

    /**
       @param socketBox should be opened and ready for comunication
    **/
    public SimpleDataChannel(Session session, SocketBox socketBox) {
        super(session);
        if (socketBox == null) {
            throw new IllegalArgumentException("socketBox is null");
        }
        if (socketBox.getSocket() == null) {
            throw new IllegalArgumentException("socket is null");
        }
        if (session == null) {
            throw new IllegalArgumentException("session is null");
        }
        this.socketBox = socketBox;
        this.transferThreadFactory = new SimpleTransferThreadFactory();
    }
    
    public void close() throws IOException {
        if (transferThread != null) {
            transferThread.interrupt();
            // wait till thread dies
            try {
                transferThread.join();
            } catch (InterruptedException e) {
            }
        }
        
        // thread should clean up after itself,
        // but let's check it
        socketBox.setSocket(null);
    }
        
    public void startTransfer(DataSink sink,
                              BasicServerControlChannel localControlChannel,
                              TransferContext context)
        throws Exception {
        transferThread =
            transferThreadFactory.getTransferSinkThread(this,
                                                        socketBox,
                                                        sink,
                                                        localControlChannel,
                                                        context);
        
        transferThread.start();
    }

    public void startTransfer(DataSource source,
                              BasicServerControlChannel localControlChannel,
                              TransferContext context)
        throws Exception {
        transferThread =
            transferThreadFactory.getTransferSourceThread(this,
                                                          socketBox,
                                                          source,
                                                          localControlChannel,
                                                          context);
        transferThread.start();
    }
}
