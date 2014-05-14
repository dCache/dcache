package diskCacheV111.doors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.util.CommandExitException;
import dmg.util.StreamEngine;

import org.dcache.cells.AbstractCell;
import org.dcache.util.Args;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.util.SequentialExecutor;
import org.dcache.util.Transfer;

/**
 * Door cell for line based protocols.
 *
 * To be used with LoginManager. The cell reads lines from a StreamEngine
 * and passes these to an interpreter for processing.
 *
 * The cell is able to detect end-of-stream even while the interpreter
 * is processing a line.
 */
public class LineBasedDoor
    extends AbstractCell implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LineBasedDoor.class);

    private final Class<? extends LineBasedInterpreter> interpreterClass;

    /**
     * Door instances are created by the LoginManager. This is the
     * stream engine passed to us from the LoginManager upon
     * instantiation.
     */
    private final StreamEngine engine;

    private LineBasedInterpreter interpreter;

    private final CountDownLatch shutdownGate = new CountDownLatch(1);

    /**
     * Executor for processing commands.
     */
    private final Executor executor;

    public LineBasedDoor(String cellName, Args args, Class<? extends LineBasedInterpreter> interpreterClass,
                         StreamEngine engine, ExecutorService executor)
    {
        super(cellName, args);

        getNucleus().setMessageExecutor(new SequentialExecutor(executor));
        this.interpreterClass = interpreterClass;
        this.engine = engine;
        this.executor = new CDCExecutorServiceDecorator<>(executor);

        try {
            doInit();
        } catch (InterruptedException e) {
            shutdownGate.countDown();
        } catch (ExecutionException e) {
            LOGGER.error(e.getCause().toString());
            shutdownGate.countDown();
        }
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();

        Transfer.initSession();

        LOGGER.debug("Client host: {}",
                engine.getInetAddress().getHostAddress());

        interpreter = interpreterClass.newInstance();
        parseOptions(interpreter);
        interpreter.setWriter(engine.getWriter());
        interpreter.setRemoteAddress((InetSocketAddress) engine.getSocket().getRemoteSocketAddress());
        interpreter.setLocalAddress((InetSocketAddress) engine.getSocket().getLocalSocketAddress());
        interpreter.setExecutor(executor);
        if (interpreter instanceof CellMessageSender) {
            ((CellMessageSender) interpreter).setCellEndpoint(this);
        }
        interpreter.init();
        if (interpreter instanceof CellCommandListener) {
            addCommandListener(interpreter);
        }
        if (interpreter instanceof CellMessageReceiver) {
            addMessageListener((CellMessageReceiver) interpreter);
        }
        executor.execute(this);
    }

    private synchronized void shutdownInputStream()
    {
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
     *
     * Commands are read from the socket and submitted to the command
     * queue for execution. Upon termination, most of the shutdown
     * logic is in this method, including:
     *
     * - Emergency shutdown of performance marker engine
     * - Shut down of passive mode server socket
     * - Abort and cleanup after failed transfers
     * - Cell shutdown initiation
     *
     * Notice that socket and thus input and output streams are not
     * closed here. See cleanUp() for details on this.
     */
    @Override
    public void run()
    {
        awaitStart();

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
                    new BufferedReader(new InputStreamReader(engine.getInputStream(), "UTF-8"));

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

    /**
     * Called by the cell infrastructure when the cell has been killed.
     *
     * The socket will be closed by this method. It is quite important
     * that this does not happen earlier, as several threads use the
     * output stream. This is the only place where we can be 100%
     * certain, that all the other threads are done with their job.
     *
     * The method blocks until the worker thread has terminated.
     */
    @Override
    public void cleanUp()
    {
        /* Closing the input stream will cause the FTP command
         * processing thread to shut down. In case the shutdown was
         * initiated by the FTP client, this will already have
         * happened at this point. However if the cell is shut down
         * explicitly, then we have to shutdown the input stream here.
         */
        shutdownInputStream();

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

        super.cleanUp();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("     User Host  : " + engine.getInetAddress().getHostAddress());
        if (interpreter instanceof CellInfoProvider) {
            ((CellInfoProvider) interpreter).getInfo(pw);
        }
    }

    public interface LineBasedInterpreter
    {
        void setWriter(Writer out);
        void execute(String cmd) throws CommandExitException;
        void init();
        void shutdown();
        void setRemoteAddress(InetSocketAddress remoteAddress);
        void setLocalAddress(InetSocketAddress localAddress);
        void setExecutor(Executor executor);
    }

    private class Command implements Runnable
    {
        private final String command;

        public Command(String command)
        {
            this.command = command;
        }

        @Override
        public void run()
        {
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
