/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.ftp.door;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import diskCacheV111.doors.TlsStarter;
import diskCacheV111.util.FsPath;

import dmg.util.CommandExitException;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * Support for ftp(e)s: an FTP door that allows the client to switch the
 * control channel to be TLS-encrypted.
 */
public class TlsFtpDoor extends WeakFtpDoorV1 implements TlsStarter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TlsFtpDoor.class);
    private Consumer<SSLEngine> startTls;
    private final SSLEngine ssl;
    private final Set<String> _plaintextCommands = new HashSet<>();
    private boolean isChannelSecure;

    /**
     * Commands that are annotated @Plaintext are allowed to be sent before the
     * TLS context is established.
     */
    @Retention(RUNTIME)
    @Target(METHOD)
    @interface Plaintext
    {
    }

    public TlsFtpDoor(SSLEngine ssl, boolean allowUsernamePassword,
            Optional<String> anonymousUser, FsPath anonymousRoot,
            boolean requireAnonPasswordEmail)
    {
        super("FTPS", allowUsernamePassword, anonymousUser, anonymousRoot,
            requireAnonPasswordEmail);
        this.ssl = ssl;

        visitFtpCommands((m, cmd) -> {
            if (m.getAnnotation(Plaintext.class) != null) {
                _plaintextCommands.add(cmd);
            }
        });
    }

    @Override
    public void setTlsStarter(Consumer<SSLEngine> startTls)
    {
        this.startTls = requireNonNull(startTls);
    }

    @Override
    public void execute(String command) throws CommandExitException
    {
        ftpcommand(command, null, isChannelSecure ? ReplyType.TLS : ReplyType.CLEAR);
    }


    @Override
    protected void checkCommandAllowed(CommandRequest command, Object commandContext)
            throws FTPCommandException
    {
        boolean isPlaintextAllowed = _plaintextCommands.contains(command.getName());

        checkFTPCommand(isChannelSecure || isPlaintextAllowed,
                530, "Command not allowed until TLS is established");

        super.checkCommandAllowed(command, commandContext);
    }

    /*  Promote the FEAT command to be available in plain-text.  This is so
     *  clients can see that AUTH is supported and learn which schemes are
     *  supported.  See RFC 4217.
     */
    @Plaintext
    @Override
    public void ftp_feat(String arg)
    {
        super.ftp_feat(arg);
    }

    @Override
    protected StringBuilder buildFeatList(StringBuilder builder)
    {
        return super.buildFeatList(builder)
                .append(' ').append("AUTH SSL TLS").append("\r\n");
    }


    @Help("AUTH <SP> <arg> - Initiate secure context negotiation.")
    @Plaintext
    public void ftp_auth(String arg) throws FTPCommandException
    {
        LOGGER.info("going to authorize");

        /* From RFC 2228 Section 3. New FTP Commands, AUTH:
         *
         *    If the server does not understand the named security
         *    mechanism, it should respond with reply code 504.
         */
        checkFTPCommand(arg.equals("TLS") || arg.equals("SSL"),
                504, "Authenticating method not supported");
        checkFTPCommand(!isChannelSecure, 534, "TLS context already established");

        startTls.accept(ssl);
        isChannelSecure = true;
        reply("234 Ready for " + arg + " handshake");
    }
}
