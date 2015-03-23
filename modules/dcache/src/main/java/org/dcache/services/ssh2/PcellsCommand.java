package org.dcache.services.ssh2;

import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.admin.LegacyAdminShell;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.applets.login.DomainObjectFrame;
import dmg.cells.nucleus.CellEndpoint;
import dmg.util.CommandException;

public class PcellsCommand implements Command, Runnable
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PcellsCommand.class);

    private final CellEndpoint _endpoint;
    private LegacyAdminShell _shell;
    private InputStream _in;
    private ExitCallback _exitCallback;
    private OutputStream _out;
    private Thread _adminShellThread;
    private ExecutorService _executor = Executors.newCachedThreadPool();
    private volatile boolean _done = false;
    private String _prompt;

    public PcellsCommand(CellEndpoint endpoint, String prompt)
    {
        _endpoint = endpoint;
        _prompt = prompt;
    }

    @Override
    public void setErrorStream(OutputStream err)
    {
        // we don't use the error stream
    }

    @Override
    public void setExitCallback(ExitCallback callback)
    {
        _exitCallback = callback;
    }

    @Override
    public void setInputStream(InputStream in)
    {
        _in = in;
    }

    @Override
    public void setOutputStream(OutputStream out)
    {
        _out = out;
    }

    @Override
    public void start(Environment env) throws IOException
    {
        String user = env.getEnv().get(Environment.ENV_USER);
        _shell = new LegacyAdminShell(user, _endpoint, _prompt);
        _adminShellThread = new Thread(this);
        _adminShellThread.start();
    }

    @Override
    public void destroy()
    {
        if (_adminShellThread != null) {
            _adminShellThread.interrupt();
        }
        _executor.shutdownNow();
    }

    @Override
    public void run() {
        try {
            final ObjectInputStream in = new ObjectInputStream(_in);
            final ObjectOutputStream out = new ObjectOutputStream(_out);
            out.flush();
            Object obj;
            while (!_done && (obj = in.readObject()) != null) {
                if (!(obj instanceof DomainObjectFrame)) {
                    LOGGER.error("Received unsupported request type: {}", obj.getClass());
                    continue;
                }

                final DomainObjectFrame frame = (DomainObjectFrame) obj;
                LOGGER.trace("Frame id {} received", frame.getId());
                _executor.execute(new Runnable() {
                    @Override
                    public void run()
                    {
                        Object result;
                        try {
                            if (frame.getDestination() == null) {
                                result = _shell.executeCommand(frame.getPayload().toString());
                            } else {
                                result = _shell.executeCommand(frame.getDestination(), frame.getPayload());
                            }
                        } catch (CommandException e) {
                            result = e;
                            _done = true;
                            try {
                                in.close();
                            } catch (IOException ignored) {
                            }
                        } catch (TimeoutCacheException e) {
                            result = null;
                        } catch (Exception ae) {
                            result = ae;
                        }
                        frame.setPayload(result);
                        try {
                            synchronized (out) {
                                out.writeObject(frame);
                                out.flush();
                                out.reset();  // prevents memory leaks...
                            }
                        } catch (IOException e) {
                            LOGGER.error("Problem sending result : {}", e);
                        }
                    }
                });
            }
        } catch (IOException e) {
            LOGGER.warn("I/O failure in pcells connection: {}", e.toString());
        } catch (ClassNotFoundException e) {
            LOGGER.error("Received unsupported request type: {}", e.getMessage());
        } finally {
            _exitCallback.onExit(0);
        }
    }
}
