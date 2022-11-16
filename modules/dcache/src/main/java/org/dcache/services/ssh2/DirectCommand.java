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
package org.dcache.services.ssh2;

import diskCacheV111.admin.UserAdminShell;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.CommandAclException;
import dmg.util.CommandEvaluationException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.channel.ChannelSession;
import org.apache.sshd.server.command.Command;
import org.dcache.cells.CellStub;
import dmg.util.PagedCommandResult;
import org.dcache.util.Strings;
import org.dcache.util.list.ListDirectoryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Command interface that allows direct command execution in admin server.
 * Executes list of commands: ssh -p PORT user@example.org "command1;command2;command3"
 *
 * @author litvinse
 */
public class DirectCommand implements Command, Runnable {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(DirectCommand.class);

    private ExitCallback exitCallback;
    private PrintWriter errorWriter;
    private PrintWriter outWriter;
    private final UserAdminShell shell;
    private List<String> commands;
    private Thread shellThread;

    DirectCommand(List<String> commands,
          CellEndpoint endpoint,
          CellStub poolManager,
          CellStub pnfsManager,
          CellStub acm,
          String prompt,
          ListDirectoryHandler list) {
        this.commands = commands;
        shell = new UserAdminShell(prompt);
        shell.setCellEndpoint(endpoint);
        shell.setPnfsManager(pnfsManager);
        shell.setPoolManager(poolManager);
        shell.setAcm(acm);
        shell.setListHandler(list);
    }

    @Override
    public void destroy(ChannelSession channelSession) {
        shellThread.interrupt();
    }

    @Override
    public void setErrorStream(OutputStream err) {
        errorWriter = new PrintWriter(new SshOutputStream(err));
    }

    @Override
    public void setExitCallback(ExitCallback callback) {
        this.exitCallback = callback;
    }

    @Override
    public void setInputStream(InputStream in) {
        //  we don't use the input stream
    }

    @Override
    public void setOutputStream(OutputStream out) {
        outWriter = new PrintWriter(new SshOutputStream(out));
    }

    @Override
    public void start(ChannelSession channelSession, Environment env) {

        try (CDC ignored = new CDC()) {
            String sessionId = Sessions.connectionId(channelSession.getServerSession());
            CDC.setSession(sessionId);
            shell.setSession(sessionId);
            shell.setUser(env.getEnv().get(Environment.ENV_USER));
            CDC cdc = new CDC();
            shellThread = new Thread(() -> cdc.execute(this));
            shellThread.start();
        }
    }

    @Override
    public void run() {
        try {
            executeCommands();
        } catch (RuntimeException e) {
            LOGGER.error(e.toString());
        } finally {
            exitCallback.onExit(0);
        }
    }

    private void executeCommands() {
        for (String command : commands) {
            Object error = null;
            try {
                while (true) {
                    Object result = shell.executeCommand(command);
                    String s;
                    if (result instanceof PagedCommandResult) {
                        s = ((PagedCommandResult) result).getPartialResult();
                    } else {
                        s = Strings.toString(result);
                    }
                    if (!s.isEmpty()) {
                        outWriter.println(s);
                    }
                    outWriter.flush();
                    if (result instanceof PagedCommandResult) {
                        PagedCommandResult pagedResult = (PagedCommandResult) result;
                        if (!pagedResult.isEOL()) {
                            pagedResult.setCommand(command);
                            command = pagedResult.nextCommand();
                            continue;
                        }
                    }
                    break;
                }
            } catch (IllegalArgumentException e) {
                error = e.toString();
            } catch (SerializationException e) {
                error = "There is a bug here, please report to support@dcache.org";
                LOGGER.error("This must be a bug, please report to support@dcache.org.", e);
            } catch (CommandSyntaxException e) {
                error = e;
            } catch (CommandEvaluationException | CommandAclException e) {
                error = e.getMessage();
            } catch (CommandExitException e) {
                break;
            } catch (CommandPanicException e) {
                error = String.format("Command '%s' triggered a bug (%s);"
                      + "the service log file contains additional information. Please "
                      + "contact support@dcache.org.", command, e.getTargetException());
            } catch (CommandException e) {
                error = e.getMessage();
            } catch (NoRouteToCellException e) {
                error = "Cell name does not exist or cell is not started: "
                      + e.getMessage();
                LOGGER.warn("Command cannot be executed in the cell, cell is gone {}",
                      e.getMessage());
            } catch (RuntimeException e) {
                error = String.format("Command '%s' triggered a bug (%s); please"
                      + " locate this message in the log file of the admin service and"
                      + " send an email to support@dcache.org with this line and the"
                      + " following stack-trace", command, e);
                LOGGER.error((String) error, e);
            } catch (Exception e) {
                error = e.getMessage();
                if (error == null) {
                    error = e.getClass().getSimpleName() + ": (null)";
                }
            }

            if (error != null) {
                if (error instanceof CommandSyntaxException) {
                    CommandSyntaxException e = (CommandSyntaxException) error;
                    errorWriter.append("Syntax error: ").println(e.getMessage());
                } else {
                    errorWriter.println(error);
                }
                errorWriter.flush();
            }
        }
    }
}
