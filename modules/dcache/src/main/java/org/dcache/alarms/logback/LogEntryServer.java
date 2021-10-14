/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */
/*
 * Logback: the reliable, generic, fast and flexible logging framework.
  Copyright (C) 1999-2013, QOS.ch. All rights reserved.

  This program and the accompanying materials are dual-licensed under
  either the terms of the Eclipse Public License v1.0 as published by
  the Eclipse Foundation

     or (per the licensee's choosing)

   under the terms of the GNU Lesser General Public License version 2.1
   as published by the Free Software Foundation.
 */
package org.dcache.alarms.logback;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import javax.net.ServerSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation adapts {@link ch.qos.logback.classic.net.SimpleSocketServer} to run directly
 * as a dCache cell component and to bypass re-entry of the remotely sent logging event into the
 * logging context.
 * <p>
 * <p>
 * This is achieved via a special implementation of the logback SocketNode which calls the
 * LogEntryHandler directly.
 *
 * @author Ceki G&uuml;lc&uuml;
 * @author S&eacute;bastien Pennec
 * @author arossi
 */
public final class LogEntryServer implements Runnable {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(LogEntryServer.class);

    private final List<LogEntryServerSocketNode> socketNodeList = new ArrayList<>();

    private LogEntryHandler handler;
    private Integer port;
    private ServerSocket serverSocket;

    public LogEntryHandler getHandler() {
        return handler;
    }

    public void run() {
        try {
            serverSocket = ServerSocketFactory.getDefault()
                  .createServerSocket(port);
        } catch (IOException t) {
            throw new RuntimeException("Failed to create the server socket.", t);
        }

        LOGGER.debug("Listening on port {}.", port);

        while (!serverSocket.isClosed()) {
            try {
                LOGGER.debug("Waiting to accept a new client.");
                Socket socket = serverSocket.accept();
                LOGGER.debug("Connected to client at {}.",
                      socket.getInetAddress());
                LOGGER.debug("Starting new socket node.");
                LogEntryServerSocketNode newSocketNode
                      = new LogEntryServerSocketNode(this, socket);
                synchronized (socketNodeList) {
                    socketNodeList.add(newSocketNode);
                }
                new Thread(newSocketNode).start();
            } catch (SocketException t) {
                if (!t.getMessage().contains("closed")) {
                    LOGGER.error("There was a problem connecting to client: {}; "
                                + "cause: {}.",
                          t.getMessage(), t.getCause());
                }
            } catch (IOException t) {
                LOGGER.error("There was a problem connecting to client: {}; "
                            + "cause: {}.",
                      t.getMessage(), t.getCause());
            }
        }
    }

    public void setHandler(LogEntryHandler handler) {
        this.handler = requireNonNull(handler);
    }

    public void setPort(Integer port) {
        checkArgument(port != null && port > 0);
        this.port = port;
    }

    public void socketNodeClosing(LogEntryServerSocketNode socketNode) {
        LOGGER.debug("Removing {}.", socketNode);
        synchronized (socketNodeList) {
            socketNodeList.remove(socketNode);
        }
    }

    /*
     * It is assumed that start and stop will not be called
     * concurrently by different threads.
     */
    public void start() {
        if (serverSocket == null || serverSocket.isClosed()) {
            new Thread(this).start();
        }
    }

    /*
     * It is assumed that start and stop will not be called
     * concurrently by different threads.
     */
    public void stop() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close serverSocket: {}.", e.getMessage());
            }

            LOGGER.debug("closing {}.", this);
            synchronized (socketNodeList) {
                for (LogEntryServerSocketNode socketNode : socketNodeList) {
                    socketNode.close();
                }
            }
        }
    }
}
