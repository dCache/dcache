package org.dcache.services.ssh2;

import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

import diskCacheV111.admin.UserAdminShell;

import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.CommandAclException;
import dmg.util.CommandEvaluationException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;

import org.dcache.util.Strings;

public class NoTerminalCommand implements Command, Runnable
{
    private static final Logger _logger =
        LoggerFactory.getLogger(NoTerminalCommand.class);
    private final UserAdminShell _userAdminShell;
    private ExitCallback _exitCallback;
    private BufferedReader _reader;
    private PrintWriter _error;
    private PrintWriter _writer;
    private Thread _adminShellThread;

    public NoTerminalCommand(UserAdminShell shell)
    {
        _userAdminShell = shell;
    }

    @Override
    public void destroy() {
    }

    @Override
    public void setErrorStream(OutputStream err) {
        _error = new PrintWriter(err);
    }

    @Override
    public void setExitCallback(ExitCallback callback) {
        _exitCallback = callback;
    }

    @Override
    public void setInputStream(InputStream in) {
        _reader = new BufferedReader(new InputStreamReader(in));
    }

    @Override
    public void setOutputStream(OutputStream out) {
        _writer = new PrintWriter(out);
    }

    @Override
    public void start(Environment env) throws IOException {
        _userAdminShell.setUser(env.getEnv().get(Environment.ENV_USER));
        _adminShellThread = new Thread(this);
        _adminShellThread.start();
    }

    @Override
    public void run() {
        try {
            repl();
        } catch (IOException e) {
            _logger.warn(e.getMessage());
        } finally {
            _exitCallback.onExit(0);
        }
    }

    private void repl() throws IOException
    {
        Ansi.setEnabled(false);
        while (true) {
            Object error = null;
            try {
                String str = _reader.readLine();
                try {
                    if (str == null) {
                        throw new CommandExitException();
                    }
                    Object result = _userAdminShell.executeCommand(str);
                    String s = Strings.toString(result);
                    if (!s.isEmpty()) {
                        _writer.println(s);
                    }
                    _writer.flush();
                } catch (IllegalArgumentException e) {
                    error = e.toString();
                } catch (SerializationException e) {
                    error =
                            "There is a bug here, please report to support@dcache.org";
                    _logger.error("This must be a bug, please report to support@dcache.org.", e);
                } catch (CommandSyntaxException e) {
                    error = e;
                } catch (CommandEvaluationException | CommandAclException e) {
                    error = e.getMessage();
                } catch (CommandExitException e) {
                    break;
                } catch (CommandPanicException e) {
                    error = "Command '" + str + "' triggered a bug (" + e.getTargetException() +
                             "); the service log file contains additional information. Please " +
                             "contact support@dcache.org.";
                } catch (CommandException e) {
                    error = e.getMessage();
                } catch (NoRouteToCellException e) {
                    error =
                            "Cell name does not exist or cell is not started: "
                            + e.getMessage();
                    _logger.warn("The cell the command was sent to is no "
                                 + "longer there: {}", e.getMessage());
                } catch (RuntimeException e) {
                    error = String.format("Command '%s' triggered a bug (%s); please" +
                                           " locate this message in the log file of the admin service and" +
                                           " send an email to support@dcache.org with this line and the" +
                                           " following stack-trace", str, e);
                    _logger.error((String) error, e);
                }
            } catch (InterruptedException e) {
                error = null;
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                error = e.getMessage();
                if (error == null) {
                    error = e.getClass().getSimpleName() + ": (null)";
                }
            }

            if (error != null) {
                if (error instanceof CommandSyntaxException) {
                    CommandSyntaxException e = (CommandSyntaxException) error;
                    _error.append("Syntax error: ").println(e.getMessage());
                } else {
                    _error.println(error);
                }
                _error.flush();
            }
        }
    }
}
