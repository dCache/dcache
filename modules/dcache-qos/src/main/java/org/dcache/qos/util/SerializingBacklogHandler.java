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
package org.dcache.qos.util;

import com.google.common.annotations.VisibleForTesting;
import diskCacheV111.vehicles.Message;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;
import org.dcache.qos.QoSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists backlogged message to a file.  When reactivated, the file is read back in and the
 * message sent back to the service. File is deleted after reload.
 */
public final class SerializingBacklogHandler implements BackloggedMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SerializingBacklogHandler.class);
    private static final String MESSAGE_SUFFIX = ".bklg";

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

    class BacklogConsumer extends BacklogThread {

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    consume();
                } catch (InterruptedException ignored) {
                    break;
                }
            }

            LOGGER.info("Backlogged messages consumer exiting.");
        }

        protected void start() {
            super.start();
        }

        protected void stop() {
            super.stop();
        }

        private void consume() throws InterruptedException {
            Collection<Serializable> data = new ArrayList<>();

            /*
             * take blocks until non-empty; it is synchronized
             */
            data.add(queue.take());

            queue.drainTo(data, 1000);

            data.stream().forEach(this::saveToBacklog);
        }

        private void saveToBacklog(Serializable message) {
            try {
                String name = UUID.randomUUID().toString() + MESSAGE_SUFFIX;
                serializeToDisk(new File(backlogStore, name), message);
            } catch (QoSException e) {
                LOGGER.error("Unable to serialize message {} to disk: {}", message, e.getMessage());
            }
        }
    }

    class Reloader extends BacklogThread {

        @Override
        public void run() {
            if (!backlogStore.exists()) {
                return;
            }

            File[] files = backlogStore.listFiles();

            for (File file : files) {
                try {
                    notifyReceivers(deserialize(file));
                } catch (QoSException e) {
                    LOGGER.warn("Deserialization failed for {}: {}; file "
                                + "is corrupt or incomplete.",
                          file, e.getMessage());
                }
                file.delete();
            }

            LOGGER.info("Done reloading backlogged messages.");
        }
    }

    final Set<Consumer<Message>> messageReceivers = new HashSet<>();
    final File backlogStore;
    final BacklogConsumer consumer;
    final Reloader reloader;

    BlockingQueue<Message> queue;

    public SerializingBacklogHandler(String path) {
        backlogStore = new File(path);
        backlogStore.mkdirs();

        consumer = new BacklogConsumer();
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
    public void saveToBacklog(Message message) {
        try {
            queue.put(message);
        } catch (InterruptedException t) {
            LOGGER.info("Queue limit reached, dropping {}.", message);
        }
    }

    @VisibleForTesting
    public void consume() throws InterruptedException {
        consumer.consume();
    }

    @VisibleForTesting
    public void reload() {
        reloader.run();
    }

    public void setReceivers(Set<Consumer<Message>> messageReceivers) {
        this.messageReceivers.addAll(messageReceivers);
    }

    @Override
    public void shutdown() {
        queue.clear();
        messageReceivers.clear();
        consumer.stop();
        reloader.stop();
    }

    private Message deserialize(File file) throws QoSException {
        try (ObjectInputStream ostream = new ObjectInputStream(new FileInputStream(file))) {
            return Message.class.cast(ostream.readObject());
        } catch (FileNotFoundException e) {
            LOGGER.warn("File not found: {}; could not deserialize.", file);
            return null;
        } catch (ClassNotFoundException e) {
            throw new QoSException("Message seems not to have been loaded!.", e);
        } catch (IOException e) {
            throw new QoSException("deserialize failed", e);
        }
    }

    private void serializeToDisk(File file, Serializable message) throws QoSException {
        try (ObjectOutputStream ostream = new ObjectOutputStream(new FileOutputStream(file))) {
            ostream.writeObject(message);
        } catch (IOException e) {
            throw new QoSException(e);
        }
    }

    private void notifyReceivers(Message message) {
        messageReceivers.stream().forEach(r -> r.accept(message));
    }
}