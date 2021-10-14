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
  Logback: the reliable, generic, fast and flexible logging framework.
  Copyright (C) 1999-2013, QOS.ch. All rights reserved.

  This program and the accompanying materials are dual-licensed under
  either the terms of the Eclipse Public License v1.0 as published by
  the Eclipse Foundation

     or (per the licensee's choosing)

   under the terms of the GNU Lesser General Public License version 2.1
   as published by the Free Software Foundation.
 */
package org.dcache.alarms.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import dmg.cells.nucleus.CDC;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import org.dcache.alarms.Alarm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@link ILoggingEvent} objects sent from a remote client using Sockets (TCP). These events
 * are passed directly to the {@link LogEntryHandler}, bypassing the logger tree.
 * <p>
 * <p>
 * This class has been adapted from {@link ch.qos.logback.classic.net.SocketNode}.
 *
 * @author Ceki G&uuml;lc&uuml;
 * @author S&eacute;bastien Pennec
 * @author Moses Hohman <mmhohman@rainbow.uchicago.edu>
 * @author arossi
 */
final class LogEntryServerSocketNode implements Runnable {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(LogEntryServerSocketNode.class);

    private final ObjectInputStream ois;
    private final Socket socket;
    private final LogEntryServer server;
    private final String hostName;

    private volatile boolean running = false;

    LogEntryServerSocketNode(LogEntryServer socketServer, Socket socket)
          throws IOException {
        this.server = socketServer;
        this.socket = socket;
        hostName = socket.getInetAddress().getCanonicalHostName();
        ois = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
    }

    public void run() {
        LogEntryHandler handler = server.getHandler();
        running = true;
        try {
            while (running) {
                ILoggingEvent event = (ILoggingEvent) ois.readObject();
                Map<String, String> properties = new HashMap<>();
                Map<String, String> mdc = event.getMDCPropertyMap();
                properties.put(Alarm.HOST_TAG, hostName);
                properties.put(Alarm.SERVICE_TAG, mdc.remove(CDC.MDC_CELL));
                properties.put(Alarm.DOMAIN_TAG, mdc.remove(CDC.MDC_DOMAIN));
                handler.handle(LoggingEventConverter.updateMDC(event, properties));
            }
        } catch (EOFException e) {
            LOGGER.trace("Benign EOF ({}).", e.getMessage());
        } catch (SocketException e) {
            LOGGER.trace("Benign Socket error ({}).", e.getMessage());
        } catch (IOException e) {
            LOGGER.debug("Error on socket node {}: {}, cause: {}.",
                  this, e.getMessage(), e.getCause());
        } catch (Exception e) {
            LOGGER.error("Unexpected exception.", e);
        }

        LOGGER.debug("Closing socket node for {}.", this);
        server.socketNodeClosing(this);
        close();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
              + ": " + socket.getRemoteSocketAddress().toString()
              + " (" + hostName + ")";
    }

    void close() {
        if (running) {
            running = false;
            try {
                ois.close();
            } catch (IOException e) {
                LOGGER.debug("Could not close connection: {}, cause: {}.",
                      e.getMessage(), e.getCause());
            }
        }
    }
}