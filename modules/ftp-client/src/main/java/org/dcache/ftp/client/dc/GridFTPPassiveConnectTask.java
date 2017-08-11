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

import java.net.ServerSocket;
import java.net.Socket;

import org.dcache.ftp.client.DataChannelAuthentication;
import org.dcache.ftp.client.DataSink;
import org.dcache.ftp.client.DataSource;
import org.dcache.ftp.client.GridFTPSession;
import org.dcache.ftp.client.vanilla.BasicServerControlChannel;

/**
 * Unlike in the parent class, here we use authentication
 * and protection.
 **/
public class GridFTPPassiveConnectTask extends PassiveConnectTask
{

    protected static final Logger logger =
            LoggerFactory.getLogger(GridFTPPassiveConnectTask.class);

    // alias to session
    final GridFTPSession gSession;

    public GridFTPPassiveConnectTask(ServerSocket myServer,
                                     DataSink sink,
                                     BasicServerControlChannel control,
                                     GridFTPSession session,
                                     DataChannelFactory factory,
                                     EBlockParallelTransferContext context)
    {
        super(myServer, sink, control, session, factory, context);
        gSession = session;
    }

    public GridFTPPassiveConnectTask(ServerSocket myServer,
                                     DataSource source,
                                     BasicServerControlChannel control,
                                     GridFTPSession session,
                                     DataChannelFactory factory,
                                     EBlockParallelTransferContext context)
    {
        super(myServer, source, control, session, factory, context);
        gSession = session;
    }

    @Override
    protected SocketBox openSocket() throws Exception
    {

        logger.debug("server.accept()");

        Socket newSocket = myServer.accept();

        logger.debug("server.accept() returned");

        if (!gSession.dataChannelAuthentication.equals(
                DataChannelAuthentication.NONE)) {
            logger.debug("authenticating");
            throw new UnsupportedOperationException("DCAU is not supported by this client.");
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

        SocketPool socketPool = ((EBlockParallelTransferContext) context).getSocketPool();
        logger.debug("adding new socket to the pool");
        socketPool.add(sBox);
        logger.debug("available cached sockets: {} ; busy: {}", socketPool.countFree(),
                     socketPool.countBusy());

        return sBox;

    }

} // class
