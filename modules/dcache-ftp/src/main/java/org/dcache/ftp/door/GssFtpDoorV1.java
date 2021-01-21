package org.dcache.ftp.door;


import com.google.common.base.Splitter;

import org.dcache.dss.DssContext;
import org.dcache.dss.DssContextFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.util.CommandExitException;

import org.dcache.auth.LoginNamePrincipal;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class GssFtpDoorV1 extends AbstractFtpDoorV1
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GssFtpDoorV1.class);
    private static final GssCommandContext SECURE_COMMAND_CONTEXT = new GssCommandContext();
    public static final String GLOBUS_URL_COPY_DEFAULT_USER =
            ":globus-mapping:";

    protected Subject subject;
    // GSS general
    protected String gssFlavor;

    protected DssContext context;
    private DssContextFactory dssContextFactory;

    private boolean _hasControlPortCleared;

    private final Set<String> _plaintextCommands = new HashSet<>();

    /**
     * Commands that are annotated @Plaintext are allowed to be sent directly,
     * as unencrypted commands.  All other commands must be sent indirectly via
     * an MIC, ENC or CONF command.
     */
    @Retention(RUNTIME)
    @Target(METHOD)
    @interface Plaintext
    {
    }

    public static class GssCommandContext
    {
    }

    public GssFtpDoorV1(String ftpDoorName, String gssFlavor, DssContextFactory dssContextFactory)
    {
        super(ftpDoorName);

        this.gssFlavor = gssFlavor;
        this.dssContextFactory = dssContextFactory;

        visitFtpCommands((m, cmd) -> {
            if (m.getAnnotation(Plaintext.class) != null) {
                _plaintextCommands.add(cmd);
            }
        });
    }

    @Override
    protected void secure_reply(CommandRequest request, String answer, String code)
    {
        byte[] allData = (answer + "\r\n").getBytes(UTF_8);

        try {
            /*
             * If the response exceeds 16.5 KiB then the encrypted output no
             * longer fits in a single TLS token and must be split into multiple
             * partial responses.  At the current time, the only place where
             * this can happen is the MLSC command for directories with more
             * than ~80 entries; the exact number depends on the file names.
             *
             * Only Globus (globus.org) uses the MLSC command and its client has
             * the peculiarity that the TLS record must match a complete number
             * of directory entries.  The Globus server code sends a TLS record
             * for each directory item when generating an MLSC response.
             */
            if (allData.length <= context.maxApplicationSize()) {
                wrapAndSend(code, ' ', allData);
            } else {
                List<String> lines = Splitter.on("\r\n").splitToList(answer);
                LOGGER.debug("Command \"{}\" response is too large, splitting it into {} lines",
                        request, lines.size());
                for (int i = 0; i < lines.size(); i++) {
                    boolean isLastLine = i == lines.size()-1;
                    byte[] lineData = (lines.get(i) + "\r\n").getBytes(UTF_8);
                    if (lineData.length > context.maxApplicationSize()) {
                        LOGGER.error("Line {} of {} is too large ({} > {})", i+1,
                                lines.size(), lineData.length, context.maxApplicationSize());
                    }
                    wrapAndSend(code, isLastLine ? ' ' : '-', lineData);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Failed to encrypt reply '{}': {}", answer, e.toString());
            reply(request.getOriginalRequest(), "500 Reply encryption error: " + e);
        }
    }

    private void wrapAndSend(String code, char separator, byte[] applicationData)
            throws IOException
    {
        byte[] wrapped = context.wrap(applicationData, 0, applicationData.length);
        println(code + separator + Base64.getEncoder().encodeToString(wrapped));
    }

    @Help("AUTH <SP> <arg> - Initiate secure context negotiation.")
    @Plaintext
    public void ftp_auth(String arg) throws FTPCommandException
    {
        LOGGER.info("GssFtpDoorV1::secure_reply: going to authorize using {}", gssFlavor);

        /* From RFC 2228 Section 3. New FTP Commands, AUTH:
         *
         *    If the server does not understand the named security
         *    mechanism, it should respond with reply code 504.
         */
        checkFTPCommand(arg.equals("GSSAPI"), 504, "Authenticating method not supported");

        /* From RFC 2228 Section 3. New FTP Commands, AUTH:
         *
         *     Some servers will allow the AUTH command to be reissued in
         *     order to establish new authentication.  [...]
         *
         * That dCache does not allow re-authentication is allowed by the
         * RFC, but the RFC does not mention which return code is to be used
         * when rejecting the command. "534 Request denied for policy
         * reasons" seems the best fit.
         */
        checkFTPCommand(context == null || !context.isEstablished(), 534, "Already authenticated");

        try {
            context = dssContextFactory.create(_remoteSocketAddress, _localSocketAddress);
        } catch (IOException e) {
            LOGGER.error("Unable to initialise service context: {}", e.toString());
            /* From RFC 2228 Section 3. New FTP Commands, AUTH:
             *
             *     If the server is not able to accept the named security
             *     mechanism, such as if a required resource is unavailable, it
             *     should respond with reply code 431.
             */
            throw new FTPCommandException(431, "Internal error");
        }
        reply("334 ADAT must follow");
    }

    @Help("ADAT <SP> <arg> - Supply context negotation data.")
    @Plaintext
    public void ftp_adat(String arg) throws FTPCommandException
    {
        checkFTPCommand(!arg.isEmpty(), 501, "ADAT must have data");
        checkFTPCommand(context != null, 503, "Send AUTH first");

        byte[] token = Base64.getDecoder().decode(arg);

        try {
            //_serviceContext.setChannelBinding(cb);
            //debug("GssFtpDoorV1::ftp_adat: CB set");
            token = context.accept(token);
            //debug("GssFtpDoorV1::ftp_adat: Token created");
            subject = context.getSubject();
            //debug("GssFtpDoorV1::ftp_adat: User principal: " + UserPrincipal);
        } catch (IOException e) {
            LOGGER.trace("Authentication failed", e);
            throw new FTPCommandException(535, "Authentication failed: " + e.getMessage());
        }
        if (token != null) {
            if (context.isEstablished()) {
                reply("235 ADAT=" + Base64.getEncoder().encodeToString(token));
            } else {
                reply("335 ADAT=" + Base64.getEncoder().encodeToString(token));
            }
        } else {
            if (context.isEstablished()) {
                LOGGER.info("GssFtpDoorV1::ftp_adat: security context established with {}", subject);
                reply("235 OK");
            } else {
                reply("335 ADAT=");
            }
        }
    }

    @Help("CCC - Switch control channel to cleartext.")
    public void ftp_ccc(String arg) throws FTPCommandException
    {
        // We should never received this, only through MIC, ENC or CONF,
        // in which case it will be intercepted by secure_command()
        throw new FTPCommandException(533, "CCC must be protected");
    }

    @ConcurrentWithTransfer
    @Help("MIC <SP> <arg> - Integrity protected command.")
    @Plaintext
    public void ftp_mic(String arg) throws CommandExitException, FTPCommandException
    {
        secure_command(arg, ReplyType.MIC);
    }

    @ConcurrentWithTransfer
    @Help("ENC <SP> <arg> - Privacy protected command.")
    @Plaintext
    public void ftp_enc(String arg) throws CommandExitException, FTPCommandException
    {
        secure_command(arg, ReplyType.ENC);
    }

    @ConcurrentWithTransfer
    @Help("CONF <SP> <arg> - Confidentiality protection command.")
    @Plaintext
    public void ftp_conf(String arg) throws CommandExitException, FTPCommandException
    {
        secure_command(arg, ReplyType.CONF);
    }

    public void secure_command(String answer, ReplyType sectype)
            throws CommandExitException, FTPCommandException
    {
        checkFTPCommand(!isNullOrEmpty(answer),
                500, "Wrong syntax of %s command", sectype);
        checkFTPCommand(context != null && context.isEstablished(),
                503, "Security context is not established");

        byte[] data = Base64.getDecoder().decode(answer);
        try {
            data = context.unwrap(data);
        } catch (IOException e) {
            LOGGER.error("GssFtpDoorV1::secure_command: got IOException: {}",
                         e.getMessage());
            throw new FTPCommandException(500, "Cannot decrypt command: " + e);
        }

        // At least one C-based client sends a zero byte at the end
        // of a secured command. Truncate trailing zeros.
        // Search from the right end of the string for a non-null character.
        int i;
        for (i = data.length; i > 0 && data[i - 1] == 0; i--) {
            //do nothing, just decrement i
        }
        String msg = new String(data, 0, i, StandardCharsets.UTF_8).trim();

        if (msg.equalsIgnoreCase("CCC")) {
            sectype = ReplyType.CLEAR;
            _hasControlPortCleared = true;
            reply("200 OK");
        } else {
            ftpcommand(msg, SECURE_COMMAND_CONTEXT, sectype);
        }
    }

    @Override
    protected void checkCommandAllowed(CommandRequest command, Object commandContext) throws FTPCommandException
    {
        boolean isSecureCommand = commandContext == SECURE_COMMAND_CONTEXT;
        boolean isPlaintextAllowed = _plaintextCommands.contains(command.getName());

        checkFTPCommand(_hasControlPortCleared || isSecureCommand || isPlaintextAllowed,
                530, "Command must be wrapped in MIC, ENC or CONF");

        super.checkCommandAllowed(command, commandContext);
    }

    @Override
    public void ftp_user(String arg) throws FTPCommandException
    {
        checkFTPCommand(!arg.isEmpty(), 500, "Missing argument");
        checkFTPCommand(context != null && context.isEstablished(),
                530, "Authentication required");

        Subject subject = context.getSubject();
        subject.getPrincipals().add(_origin);
        if (!arg.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
            subject.getPrincipals().add(new LoginNamePrincipal(arg));
        }

        try {
            login(subject);
            reply("200 User " + arg + " logged in");
        } catch (PermissionDeniedCacheException e) {
            LOGGER.warn("Login denied for {}: {}", context.getPeerName(), e.getMessage());
            throw new FTPCommandException(530, "Login denied");
        } catch (CacheException e) {
            LOGGER.error("Login failed for {}: {}", context.getPeerName(), e.getMessage());
            throw new FTPCommandException(530, "Login failed: " + e.getMessage());
        }
    }

    // Some clients, even though the user is already logged in via GSS and ADAT,
    // will send a dummy PASS anyway. "Already logged in" is distracting
    // and the "Going to evaluate strong password" message is misleading
    // since nothing is actually done for this command.
    // Example = ubftp client
    @Override
    public void ftp_pass(String arg) throws FTPCommandException
    {
        LOGGER.debug("GssFtpDoorV1::ftp_pass: PASS is a no-op with " +
                     "GSSAPI authentication.");
        checkFTPCommand(subject != null, 500, "Send USER first");

        reply(ok("PASS"));
    }
}
