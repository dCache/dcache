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
package org.dcache.resilience.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import org.dcache.resilience.handlers.ResilienceMessageHandler;
import org.dcache.vehicles.CorruptFileMessage;

/**
 * <p>Persists backlogged messages to a file.  When reactivated, the
 *      file is read back in and the messages sent back to the
 *      {@link org.dcache.resilience.handlers.ResilienceMessageHandler}.</p>
 *
 * <p>Pool-based messages are not saved.</p>
 *
 * <p>File is deleted after reload.</p>
 */
public final class SimplePersistentBacklogHandler
                implements BackloggedMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    SimplePersistentBacklogHandler.class);

    abstract class BacklogThread implements Runnable {
        private Thread thread;

        protected void start() {
            if (thread == null || !thread.isAlive()) {
                thread = new Thread(this, this.getClass().getSimpleName());
                thread.start();
            }
        }

        protected void stop() {
            if (thread != null && thread.isAlive()) {
                thread.interrupt();
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            thread = null;
        }
    }

    class Consumer extends BacklogThread {
        private PrintWriter pw;

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    Collection<Serializable> data = new ArrayList<>();

                    /*
                     * take blocks until non-empty; it is synchronized
                     */
                    data.add(queue.take());

                    queue.drainTo(data, 1000);

                    data.stream().forEach(this::saveToBacklog);
                } catch (InterruptedException ignored) {
                    break;
                }
            }

            LOGGER.info("Backlogged messages consumer exiting.");
        }

        protected void start() {
            try {
                pw = new PrintWriter(new FileWriter(backlogStore, true));
            } catch (IOException e) {
                LOGGER.info("Failed to open writer to save "
                                + "backlogged messages: {}.",
                                new ExceptionMessage(e));
                return;
            }
            super.start();
        }

        protected void stop() {
            super.stop();
            pw.close();
        }

        /**
         * <p>Only pnfs-based messages are stored.</p>
         */
        private void saveToBacklog(Serializable message) {
            String pnfsid;
            String pool;
            String type;

            if (message instanceof CorruptFileMessage) {
                CorruptFileMessage specific = (CorruptFileMessage) message;
                pnfsid = specific.getPnfsId().toString();
                pool = specific.getPool();
                type = CorruptFileMessage.class.getSimpleName();
            } else if (message instanceof PnfsClearCacheLocationMessage) {
                PnfsClearCacheLocationMessage specific
                                = (PnfsClearCacheLocationMessage) message;
                pnfsid = specific.getPnfsId().toString();
                pool = specific.getPoolName();
                type = PnfsClearCacheLocationMessage.class.getSimpleName();
            } else if (message instanceof PnfsAddCacheLocationMessage) {
                PnfsAddCacheLocationMessage specific
                                = (PnfsAddCacheLocationMessage) message;
                pnfsid = specific.getPnfsId().toString();
                pool = specific.getPoolName();
                type = PnfsAddCacheLocationMessage.class.getSimpleName();
            } else {
                return;
            }

            pw.println(type + "," + pnfsid + "," + pool);

            if (pw.checkError()) {
                LOGGER.error("Problem saving ({} {} {}) to file; skipped.",
                                type, pnfsid, pool);
            }
        }

    }

    class Reloader extends BacklogThread {
        @Override
        public void run() {
            if (!backlogStore.exists()) {
                return;
            }

            try (BufferedReader fr
                            = new BufferedReader(new FileReader(backlogStore))) {
                while (!Thread.interrupted()) {
                    String line = fr.readLine();
                    if (line == null) {
                        break;
                    }

                    try {
                        messageHandler.processBackloggedMessage(fromString(line));
                    } catch (IllegalStateException e) {
                        LOGGER.debug("Unable to reload message; {}",
                                        e.getMessage());
                    }
                }

                backlogStore.delete();
            } catch (FileNotFoundException e) {
                LOGGER.error("Unable to reload checkpoint file: {}",
                                e.getMessage());
            } catch (IOException e) {
                LOGGER.error("Unrecoverable error during reload checkpoint file: {}",
                                e.getMessage());
            }

            LOGGER.info("Done reloading backlogged messages.");
        }
    }

    private static Message fromString(String line) {
        String[] parts = line.split("[,]");
        if (parts.length != 3) {
            throw new IllegalStateException(line + " is malformed.");
        }

        if (parts[0].equals(CorruptFileMessage.class.getSimpleName())) {
            return new CorruptFileMessage(parts[2], new PnfsId(parts[1]));
        } else if (parts[0].equals(
                        PnfsClearCacheLocationMessage.class.getSimpleName())) {
            return new PnfsClearCacheLocationMessage(new PnfsId(parts[1]),
                            parts[2]);
        } else if (parts[0].equals(
                        PnfsAddCacheLocationMessage.class.getSimpleName())) {
            return new PnfsAddCacheLocationMessage(new PnfsId(parts[1]),
                            parts[2]);
        } else {
            throw new IllegalStateException(line + ": invalid message type.");
        }
    }

    BlockingQueue<Serializable> queue;
    ResilienceMessageHandler messageHandler;

    private final File backlogStore;
    private final Consumer consumer;
    private final Reloader reloader;

    public SimplePersistentBacklogHandler(String path) {
        backlogStore = new File(path);
        consumer = new Consumer();
        reloader = new Reloader();
         /*
         *  Set the max fairly high.  File write should not take
         *  that long.
         */
        queue = new LinkedBlockingDeque<>(1000000);
    }

    @Override
    public void handleBacklog() {
        consumer.stop();
        reloader.start();
    }

    @Override
    public void initialize() {
        consumer.start();
    }

    @Override
    public void saveToBacklog(Serializable message) {
        try {
            queue.put(message);
        } catch (InterruptedException t) {
            LOGGER.info("Queue limit reached, dropping {}.", message);
        }
    }

    public void setMessageHandler(ResilienceMessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    @Override
    public void shutdown() {
        queue.clear();
        consumer.stop();
        reloader.stop();
    }
}