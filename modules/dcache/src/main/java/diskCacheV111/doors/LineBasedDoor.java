/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 - 2021 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package diskCacheV111.doors;

import static java.nio.charset.StandardCharsets.UTF_8;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandExitException;
import dmg.util.KeepAliveListener;
import dmg.util.StreamEngine;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import org.dcache.cells.AbstractCell;
import org.dcache.poolmanager.PoolManagerHandler;
import org.dcache.util.Args;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.util.SequentialExecutor;
import org.dcache.util.Transfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Door cell for line based protocols.
 *
 * <p>To be used with LoginManager. The cell reads lines from a StreamEngine
 * and passes these to an interpreter for processing.
 *
 * <p>The cell is able to detect end-of-stream even while the interpreter
 * is processing a line.
 */
public class LineBasedDoor
      extends AbstractCell implements Runnable, KeepAliveListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(LineBasedDoor.class);

    /**
     * Door instances are created by the LoginManager. This is the stream engine passed to us from
     * the LoginManager upon instantiation.
     */
    private final StreamEngine engine;

    private final LineBasedInterpreterFactory factory;

    private final CountDownLatch shutdownGate = new CountDownLatch(1);

    /**
     * Executor for processing commands.
     */
    private final Executor executor;

    private final PoolManagerHandler poolManager;

    private LineBasedInterpreter interpreter;
    private volatile boolean isStartupCompleted;

    public LineBasedDoor(String cellName, Args args, LineBasedInterpreterFactory factory,
          StreamEngine engine, ExecutorService executor, PoolManagerHandler poolManagerHandler) {
        super(cellName, args, executor);

        this.factory = factory;
        this.engine = engine;
        this.executor = new CDCExecutorServiceDecorator<>(executor);
        this.poolManager = poolManagerHandler;
    }

    @Override
    protected void starting()
          throws Exception {
        Transfer.initSession(false, true);
        super.starting();

        LOGGER.debug("Client host: {}", engine.getInetAddress().getHostAddress());

        interpreter = factory.create(this, getNucleus().getThisAddress(), engine, executor,
              poolManager);
        if (interpreter instanceof CellCommandListener) {
            addCommandListener(interpreter);
        }
        if (interpreter instanceof CellMessageReceiver) {
            addMessageListener((CellMessageReceiver) interpreter);
        }
    }

    @Override
    protected void started() {
        executor.execute(this);
        isStartupCompleted = true;
    }

    private synchronized void shutdownInputStream() {
        try {
            Socket socket = engine.getSocket();
            if (!socket.isClosed() && !socket.isInputShutdown()) {
                socket.shutdownInput();
            }
        } catch (IOException e) {
            LOGGER.info("Failed to shut down input stream of the " +
                  "control channel: {}", e.getMessage());
        }
    }

    /**
     * Main loop for command processing.
     * <p>
     * Commands are read from the socket and submitted to the command queue for execution. Upon
     * termination, most of the shutdown logic is in this method, including:
     * <p>
     * - Emergency shutdown of performance marker engine - Shut down of passive mode server socket -
     * Abort and cleanup after failed transfers - Cell shutdown initiation
     * <p>
     * Notice that socket and thus input and output streams are not closed here. See cleanUp() for
     * details on this.
     */
    @Override
    public void run() {
        try {
            SequentialExecutor executor = new SequentialExecutor(this.executor);
            try {
                /* Notice that we do not close the input stream, as
                 * doing so would close the socket as well. We don't
                 * want to do that until cleanUp() is called.
                 *
                 * REVISIT: I hope that the StreamEngine does not
                 * maintain any resources that do not get
                 * automatically freed when the socket is closed.
                 */
                BufferedReader in =
                      new BufferedReader(new InputStreamReader(engine.getInputStream(), UTF_8));

                String s = in.readLine();
                while (s != null) {
                    executor.execute(new Command(s));
                    s = in.readLine();
                }
            } catch (IOException e) {
                LOGGER.error("Got error reading data: {}", e.getMessage());
            } finally {
                try {
                    executor.shutdownNow();
                    interpreter.shutdown();
                    executor.awaitTermination();
                } catch (InterruptedException e) {
                    LOGGER.error("Failed to shut down command processing: {}",
                          e.getMessage());
                }

                LOGGER.debug("End of stream encountered");
            }
        } finally {
            /* cleanUp() waits for us to open the gate.
             */
            shutdownGate.countDown();

            /* Killing the cell will cause cleanUp() to be
             * called (although from a different thread).
             */
            kill();
        }
    }

    public void messageArrived(NoRouteToCellException e) {
        LOGGER.warn(e.getMessage());
    }

    /**
     * Called by the cell infrastructure when the cell has been killed.
     * <p>
     * The socket will be closed by this method. It is quite important that this does not happen
     * earlier, as several threads use the output stream. This is the only place where we can be
     * 100% certain, that all the other threads are done with their job.
     * <p>
     * The method blocks until the worker thread has terminated.
     */
    @Override
    public void stopped() {
        interpreter.messagingClosed();

        /* Closing the input stream will cause the FTP command
         * processing thread to shut down. In case the shutdown was
         * initiated by the FTP client, this will already have
         * happened at this point. However if the cell is shut down
         * explicitly, then we have to shutdown the input stream here.
         */
        shutdownInputStream();

        if (isStartupCompleted) {
            /* The FTP command processing thread will open the gate after
             * shutdown.
             */
            try {
                shutdownGate.await();
            } catch (InterruptedException e) {
                /* This should really not happen as nobody is supposed to
                 * interrupt the cell thread, but if it does happen then
                 * we better log it.
                 */
                LOGGER.error("Got interrupted exception shutting down input stream");
            }
        }

        try {
            /* Closing the socket will also close the input and output
             * streams of the socket. This in turn will cause the
             * command poller thread to shut down.
             */
            engine.getSocket().close();
        } catch (IOException e) {
            LOGGER.error("Got I/O exception closing socket: {}",
                  e.getMessage());
        }

        super.stopped();
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.println("     User Host  : " + engine.getInetAddress().getHostAddress());
        if (interpreter instanceof CellInfoProvider) {
            ((CellInfoProvider) interpreter).getInfo(pw);
        }
    }

    @Override
    public void keepAlive() {
        if (interpreter instanceof KeepAliveListener) {
            KeepAliveListener.class.cast(interpreter).keepAlive();
        }
    }

    private class Command implements Runnable {

        private final String command;

        public Command(String command) {
            this.command = command;
        }

        @Override
        public void run() {
            try {
                interpreter.execute(command);
            } catch (CommandExitException e) {
                shutdownInputStream();
            } catch (RuntimeException e) {
                LOGGER.error("Bug detected", e);
            }
        }
    }
}
