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

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import org.dcache.ftp.client.DataSink;
import org.dcache.ftp.client.DataSource;
import org.dcache.ftp.client.HostPort;
import org.dcache.ftp.client.Session;
import org.dcache.ftp.client.vanilla.BasicServerControlChannel;
import org.dcache.ftp.client.vanilla.FTPServerFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes a connection to a remote data channel (FTPClient use only).
 **/
public class ActiveConnectTask extends Task {

    protected static final Logger logger =
          LoggerFactory.getLogger(ActiveConnectTask.class);

    protected HostPort hostPort;
    protected SocketBox mySocketBox;
    protected DataSink sink;
    protected DataSource source;
    protected BasicServerControlChannel control;
    protected Session session;
    protected DataChannelFactory factory;
    protected TransferContext context;

    public ActiveConnectTask(HostPort hostPort,
          DataSink sink,
          BasicServerControlChannel control,
          Session session,
          DataChannelFactory factory,
          TransferContext context) {
        this.sink = sink;
        init(hostPort, control, session, factory, context);
    }

    public ActiveConnectTask(HostPort hostPort,
          DataSource source,
          BasicServerControlChannel control,
          Session session,
          DataChannelFactory factory,
          TransferContext context) {
        this.source = source;
        init(hostPort, control, session, factory, context);
    }

    private void init(HostPort hostPort,
          BasicServerControlChannel control,
          Session session,
          DataChannelFactory factory,
          TransferContext context) {
        this.hostPort = hostPort;
        this.session = session;
        this.control = control;
        this.factory = factory;
        this.context = context;
    }

    @Override
    public void execute() {
        try {
            DataChannel dataChannel = null;
            mySocketBox = null;
            try {
                mySocketBox = openSocket();
            } catch (Exception e) {
                FTPServerFacade.exceptionToControlChannel(
                      e,
                      "active connection failed",
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
                      "active connection to server failed",
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
     * Override this to implement authentication
     **/
    protected SocketBox openSocket() throws Exception {
        SocketBox sBox = new SimpleSocketBox();
        sBox.setSocket(
              SocketChannel.open(new InetSocketAddress(hostPort.getHost(), hostPort.getPort()))
                    .socket());
        return sBox;
    }

    private void close() {
        // server will by closed by the FTPServerFacade.
        //try { server.close(); } catch (Exception ignore) {}
    }

    @Override
    public void stop() {
        close();
    }
}
