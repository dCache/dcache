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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import org.dcache.ftp.client.DataChannelAuthentication;
import org.dcache.ftp.client.GridFTPSession;
import org.dcache.ftp.client.HostPort;
import org.dcache.ftp.client.extended.GridFTPServerFacade;
import org.dcache.ftp.client.vanilla.BasicServerControlChannel;
import org.dcache.ftp.client.vanilla.FTPServerFacade;

/**
 * Unlike in the parent class, here we use authentication
 * and protection.
 **/
public class GridFTPActiveConnectTask extends Task
{

    private static final Logger logger =
            LoggerFactory.getLogger(GridFTPActiveConnectTask.class);

    protected HostPort hostPort;
    protected BasicServerControlChannel control;
    protected SocketBox box;
    protected GridFTPSession gSession;

    public GridFTPActiveConnectTask(HostPort hostPort,
                                    BasicServerControlChannel control,
                                    SocketBox box,
                                    GridFTPSession gSession)
    {
        if (box == null) {
            throw new IllegalArgumentException("Socket box is null");
        }
        this.hostPort = hostPort;
        this.control = control;
        this.box = box;
        this.gSession = gSession;
    }

    @Override
    public void execute()
    {
        Socket mySocket = null;

        if (logger.isDebugEnabled()) {
            logger.debug("connecting new socket to: " +
                         hostPort.getHost() + " " + hostPort.getPort());
        }

        try {
            mySocket = SocketChannel.open(new InetSocketAddress(hostPort.getHost(), hostPort.getPort())).socket();

            if (!gSession.dataChannelAuthentication.equals(
                    DataChannelAuthentication.NONE)) {

                logger.debug("authenticating");
                mySocket = GridFTPServerFacade.authenticate(mySocket,
                                                            true,
                                                            // this IS client socket
                                                            gSession.credential,
                                                            gSession.dataChannelProtection,
                                                            gSession.dataChannelAuthentication);

            } else {
                logger.debug("not authenticating");
            }

            // setting the Facade's socket list

            // synchronize to prevent race condidion against
            // the section in GridFTPServerFacade.setTCPBufferSize
            synchronized (box) {
                box.setSocket(mySocket);
            }

        } catch (Exception e) {
            FTPServerFacade.exceptionToControlChannel(
                    e,
                    "active connection to server failed",
                    control);
            try {
                if (mySocket != null) {
                    mySocket.close();
                }
            } catch (Exception second) {
            }
        }
    }
} // class
