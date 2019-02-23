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
package dmg.cells.services.login;

import com.google.common.util.concurrent.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellEndpoint;
import dmg.util.StreamEngine;

import org.dcache.auth.Subjects;
import org.dcache.util.Args;

public abstract class StreamEngineLoginCellFactory extends AbstractService implements LoginCellFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoginManager.class);

    private static final Class<?>[] AUTH_CON_SIGNATURE =
            { CellEndpoint.class, Args.class };

    private final Args args;

    private final String protocol;
    private final Constructor<?> authConstructor;
    private final Class<?> authClass;

    private final CellEndpoint endpoint;

    public StreamEngineLoginCellFactory(Args args, CellEndpoint endpoint)
    {
        this.args = args;
        this.endpoint = endpoint;

        protocol = checkProtocol(args.getOpt("prot"));
        LOGGER.info("Using protocol : {}", protocol);

        try {
            authClass = toAuthClass(args.getOpt("auth"), protocol);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("No such class: " + args.getOpt("auth"));
        }

        Constructor<?> authConstructor;
        if (authClass != null) {
            try {
                authConstructor = authClass.getConstructor(AUTH_CON_SIGNATURE);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Class lacks authentication constructor: " + authClass);
            }
            LOGGER.trace("Using authentication constructor: {}", authConstructor);
        } else {
            authConstructor = null;
            LOGGER.trace("No authentication used");
        }

        this.authConstructor = authConstructor;
    }

    private static String checkProtocol(String protocol) throws IllegalArgumentException
    {
        if (protocol == null) {
            protocol = "telnet";
        }
        if (!protocol.equals("telnet") && !protocol.equals("raw")) {
            throw new IllegalArgumentException("Protocol must be telnet or raw");
        }
        return protocol;
    }

    private static Class<?> toAuthClass(String authClassName, String protocol) throws ClassNotFoundException
    {
        Class<?> authClass = null;
        if (authClassName == null) {
            switch (protocol) {
            case "raw":
                authClass = null;
                break;
            case "telnet":
                authClass = TelnetSAuth_A.class;
                break;
            }
        } else if (!authClassName.equals("none")) {
            authClass = Class.forName(authClassName);
        }
        if (authClass != null) {
            LOGGER.info("Using authentication Module: {}", authClass);
        }
        return authClass;
    }

    @Override
    public Cell newCell(Socket socket) throws InvocationTargetException
    {
        StreamEngine engine;
        try {
            if (authConstructor != null) {
                engine = StreamEngineFactory.newStreamEngine(socket, protocol, endpoint, args);
            } else {
                engine = StreamEngineFactory.newStreamEngineWithoutAuth(socket, protocol);
            }
        } catch (Exception e) {
            throw new InvocationTargetException(e, "Failed to instantiate stream engine: " + e);
        }

        String userName = Subjects.getDisplayName(engine.getSubject());
        LOGGER.info("connection created for user {}", userName);

        int p = userName.indexOf('@');
        if (p > -1) {
            userName = p == 0 ? "unknown" : userName.substring(0, p);
        }

        return newCell(engine, userName);
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("  Factory        : " + getClass());
        pw.println("  Encoding       : " + protocol);
        if (authClass != null) {
            pw.println("  Authentication : " + authClass);
        }
    }

    public abstract Cell newCell(StreamEngine engine, String userName)
            throws InvocationTargetException;
}
