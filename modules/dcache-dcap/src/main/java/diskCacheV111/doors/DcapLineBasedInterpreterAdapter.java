/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.PrintWriter;
import java.net.InetAddress;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.util.CommandExitException;
import dmg.util.StreamEngine;

import org.dcache.auth.Subjects;
import org.dcache.util.Args;

/**
 * Class to adapt the DCapDoorInterpreterV3 to the LineBasedInterpreter interface.
 *
 * @see DcapInterpreterFactory
 * @see DCapDoorInterpreterV3
 */
public class DcapLineBasedInterpreterAdapter implements LineBasedInterpreter, CellMessageReceiver, CellInfoProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DcapLineBasedInterpreterAdapter.class);

    private final DCapDoorInterpreterV3 interpreter;

    private final PrintWriter out;

    private final InetAddress clientAddress;

    private final Subject subject;

    private String lastCommand;

    private int commandCounter;

    private boolean hasExited;

    public DcapLineBasedInterpreterAdapter(CellEndpoint endpoint, CellAddressCore myAddress, StreamEngine engine, Args args)
    {
        out = new PrintWriter(engine.getWriter(), true);
        clientAddress = engine.getInetAddress();
        subject = engine.getSubject();
        interpreter = new DCapDoorInterpreterV3(endpoint, myAddress, new Args(args), out, subject, clientAddress);
    }

    @Override
    public void execute(String cmd) throws CommandExitException
    {
        LOGGER.info("Executing command: {}", cmd);

        commandCounter++;
        lastCommand = cmd;

        VspArgs args;
        try {
            args = new VspArgs(cmd);
        } catch (IllegalArgumentException e) {
            interpreter.println("protocol violation: " + e.getMessage());
            LOGGER.debug("protocol violation [{}] from {}", e.getMessage(), clientAddress);
            throw new CommandExitException("Protocol violation");
        }

        try {
            String answer = interpreter.execute(args);
            if (answer != null) {
                LOGGER.info("Our answer : {}", answer);
                interpreter.println(answer);
            }
        } catch (CommandExitException e) {
            hasExited = true;
            LOGGER.info("ComThread : protocol ended");
            throw e;
        }
    }

    @Override
    public void shutdown()
    {
        interpreter.close();
        if (hasExited) {
            interpreter.println("0 0 server byebye");
        } else {
            interpreter.println("0 0 server shutdown");
        }
    }

    public void messageArrived(CellMessage envelope, Object msg)
    {
        interpreter.messageArrived(envelope);
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("         User  : " + Subjects.getDisplayName(subject));
        pw.println("         Host  : " + clientAddress);
        pw.println(" Last Command  : " + lastCommand);
        pw.println(" Command Count : " + commandCounter);
        interpreter.getInfo(pw);
    }
}
