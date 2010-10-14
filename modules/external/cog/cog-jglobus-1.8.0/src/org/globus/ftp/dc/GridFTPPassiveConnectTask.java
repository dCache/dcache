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

import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.DataSource;
import org.globus.ftp.DataSink;
import org.globus.ftp.Session;
import org.globus.ftp.vanilla.BasicServerControlChannel;
import org.globus.ftp.extended.GridFTPServerFacade;

import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   Unlike in the parent class, here we use authentication
   and protection.
 **/
public class GridFTPPassiveConnectTask extends PassiveConnectTask {

    protected static Log logger = 
        LogFactory.getLog(GridFTPPassiveConnectTask.class.getName());

    // alias to session
    GridFTPSession gSession;

    public GridFTPPassiveConnectTask(ServerSocket myServer, 
                                     DataSink sink,
                                     BasicServerControlChannel control,
                                     GridFTPSession session,
                                     DataChannelFactory factory,
                                     EBlockParallelTransferContext context)  {
        super(myServer, sink, control, session, factory, context);
        gSession = session;
    }
        
    public GridFTPPassiveConnectTask(ServerSocket myServer, 
                                     DataSource source,
                                     BasicServerControlChannel control,
                                     GridFTPSession session,
                                     DataChannelFactory factory,
                                     EBlockParallelTransferContext context) {
        super(myServer, source, control, session, factory, context);
        gSession = session;
    }
    
    protected SocketBox openSocket() throws Exception{
        
        logger.debug("server.accept()");

        Socket newSocket = myServer.accept();

        // set TCP buffer size
        if (gSession.TCPBufferSize != Session.SERVER_DEFAULT) {
            logger.debug("setting socket's TCP buffer size to " 
                         + gSession.TCPBufferSize);
            newSocket.setReceiveBufferSize(gSession.TCPBufferSize);
            newSocket.setSendBufferSize(gSession.TCPBufferSize);
        }
        
        logger.debug("server.accept() returned");
        
        if (!gSession.dataChannelAuthentication.equals(
                           DataChannelAuthentication.NONE)) {
            logger.debug("authenticating");
            newSocket = GridFTPServerFacade.authenticate(newSocket,
                                                         false, // this is NOT client socket
                                                         gSession.credential,
                                                         gSession.dataChannelProtection,
                                                         gSession.dataChannelAuthentication);
        } else {
            // do not authenticate
            logger.debug("not authenticating");
        }

        // mark the socket as busy and store in the global socket pool
            
        ManagedSocketBox sBox = new ManagedSocketBox();
        sBox.setSocket(newSocket);
        sBox.setStatus(ManagedSocketBox.BUSY);
            
        if (session.transferMode != GridFTPSession.MODE_EBLOCK) {
                            
            // synchronize to prevent race condidion against
            // the section in GridFTPServerFacade.setTCPBufferSize
            synchronized (sBox) {
                sBox.setReusable(false);
            }
        }
        
        SocketPool socketPool = ((EBlockParallelTransferContext)context).getSocketPool();
        logger.debug("adding new socket to the pool");
        socketPool.add(sBox);
        logger.debug("available cached sockets: " + socketPool.countFree()  
                     + "; busy: " + socketPool.countBusy());

        return sBox;
        
    }

} // class
