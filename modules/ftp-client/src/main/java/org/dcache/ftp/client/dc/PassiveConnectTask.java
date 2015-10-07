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

import org.dcache.ftp.client.Session;
import org.dcache.ftp.client.DataSource;
import org.dcache.ftp.client.DataSink;
import org.dcache.ftp.client.GridFTPSession;
import org.dcache.ftp.client.vanilla.FTPServerFacade;
import org.dcache.ftp.client.vanilla.BasicServerControlChannel;

import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
   This task will wait on the local server for the new incoming connection 
   and when it comes it will start a new transfer thread on the new connection.
   It is little tricky: it will cause data channel to start
   a new thread. By the time this task completes, the new
   thread is running the transfer.

   Any resulting exceptions are piped to the local control channel.

 **/
public class PassiveConnectTask extends Task {

    protected static Logger logger =
        LoggerFactory.getLogger(PassiveConnectTask.class);

    protected ServerSocket myServer;
    protected SocketBox mySocketBox;
    protected DataSink sink;
    protected DataSource source;
    protected BasicServerControlChannel control;
    protected Session session;
    protected DataChannelFactory factory;
    protected TransferContext context;
    
    public PassiveConnectTask(ServerSocket myServer,
                              DataSink sink,
                              BasicServerControlChannel control,
                              Session session,
                              DataChannelFactory factory,
                              TransferContext context) {
        this.sink = sink;
        init(myServer, control, session, factory, context);
    }
    
    public PassiveConnectTask(ServerSocket myServer,
                              DataSource source,
                              BasicServerControlChannel control,
                              Session session,
                              DataChannelFactory factory,
                              TransferContext context) {
        this.source = source;
        init(myServer, control, session, factory, context);
    }

    private void init(ServerSocket myServer,
                      BasicServerControlChannel control,
                      Session session,
                      DataChannelFactory factory,
                      TransferContext context) {
        if (!(session.serverMode == Session.SERVER_PASSIVE
              || session.serverMode == GridFTPSession.SERVER_EPAS)) {
            throw new IllegalStateException();
        }
        
        if (myServer == null) {
            throw new IllegalArgumentException("server is nul");
        }
        this.session = session;
        this.myServer = myServer;
        this.control = control;
        this.factory = factory;
        this.context = context;
    }

    public void execute() {
        try {
            DataChannel dataChannel = null;
            mySocketBox = null;
            try {
                mySocketBox = openSocket();
            } catch (Exception e) {
                FTPServerFacade.exceptionToControlChannel(
                        e,
                        "server.accept() failed",
                        control);
                return;
            }

            try {
                dataChannel = factory.getDataChannel(session, mySocketBox);
                if (sink != null) {
                    logger.debug("starting sink data channel");
                    dataChannel.startTransfer(sink, control, context);
                } else if (source != null) {
                    logger.debug("starting source data channel");
                    dataChannel.startTransfer(source, control, context);
                } else {
                    logger.error("not set");
                }

            } catch (Exception e) {
                FTPServerFacade.exceptionToControlChannel(
                                        e,
                                        "startTransfer() failed: ",
                                        control);
                if (dataChannel != null) {
                    dataChannel.close();
                }
            }

        } catch (Exception e) {
            FTPServerFacade.cannotPropagateError(e);
        }
    }

    /**
       Override this to implement authentication
    **/
    protected SocketBox openSocket() throws Exception {
        logger.debug("server.accept()");
        
        SocketBox sBox = new SimpleSocketBox();
        Socket newSocket = myServer.accept();
        sBox.setSocket(newSocket);
        
        return sBox;
    }
    
    private void close() {
        // server will by closed by the FTPServerFacade.
        try { myServer.close(); } catch (Exception ignore) {}
    }
    
    public void stop() {
        close();
    }
}
