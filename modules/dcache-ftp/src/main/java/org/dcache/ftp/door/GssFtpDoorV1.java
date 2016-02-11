package org.dcache.ftp.door;

import com.google.common.base.Throwables;
import org.dcache.dss.DssContext;
import org.dcache.dss.DssContextFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.cert.CertPathValidatorException;
import java.util.Base64;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.util.CommandExitException;

import org.dcache.auth.LoginNamePrincipal;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;

public abstract class GssFtpDoorV1 extends AbstractFtpDoorV1
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GssFtpDoorV1.class);

    public static final String GLOBUS_URL_COPY_DEFAULT_USER =
            ":globus-mapping:";

    private static final Charset UTF8 = Charset.forName("UTF-8");

    protected Subject subject;
    // GSS general
    protected String gssFlavor;

    protected DssContext context;
    private DssContextFactory dssContextFactory;

    public GssFtpDoorV1(String ftpDoorName, String tlogName, String gssFlavor, DssContextFactory dssContextFactory)
    {
        super(ftpDoorName, tlogName);
        this.gssFlavor = gssFlavor;
        this.dssContextFactory = dssContextFactory;
    }

    @Override
    protected void secure_reply(String answer, String code)
    {
        answer = answer + "\r\n";
        byte[] data = answer.getBytes(UTF8);
        try {
            data = context.wrap(data, 0, data.length);
        } catch (IOException e) {
            reply("500 Reply encryption error: " + e);
            return;
        }
        println(code + " " + Base64.getEncoder().encodeToString(data));
    }

    @Help("AUTH <SP> <arg> - Initiate secure context negotiation.")
    public void ftp_auth(String arg) throws FTPCommandException
    {
        LOGGER.info("GssFtpDoorV1::secure_reply: going to authorize using {}", gssFlavor);
        if (!arg.equals("GSSAPI")) {
            /* From RFC 2228 Section 3. New FTP Commands, AUTH:
             *
             *    If the server does not understand the named security
             *    mechanism, it should respond with reply code 504.
             */
            throw new FTPCommandException(504, "Authenticating method not supported");
        }
        if (context != null && context.isEstablished()) {
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
            throw new FTPCommandException(534, "Already authenticated");
        }

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
    public void ftp_adat(String arg)
    {
        if (arg == null || arg.length() <= 0) {
            reply("501 ADAT must have data");
            return;
        }

        if (context == null) {
            reply("503 Send AUTH first");
            return;
        }
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
            reply("535 Authentication failed: " + e.getMessage());
            return;
        }
        if (token != null) {
            if (!context.isEstablished()) {
                reply("335 ADAT=" + Base64.getEncoder().encodeToString(token));
            } else {
                reply("235 ADAT=" + Base64.getEncoder().encodeToString(token));
            }
        } else {
            if (!context.isEstablished()) {
                reply("335 ADAT=");
            } else {
                LOGGER.info("GssFtpDoorV1::ftp_adat: security context established with {}", subject);
                reply("235 OK");
            }
        }
    }

    @Help("CCC - Switch control channel to cleartext.")
    public void ftp_ccc(String arg)
    {
        // We should never received this, only through MIC, ENC or CONF,
        // in which case it will be intercepted by secure_command()
        reply("533 CCC must be protected");
    }

    @Help("MIC <SP> <arg> - Integrity protected command.")
    public void ftp_mic(String arg)
            throws CommandExitException
    {
        secure_command(arg, "mic");
    }

    @Help("ENC <SP> <arg> - Privacy protected command.")
    public void ftp_enc(String arg)
            throws CommandExitException
    {
        secure_command(arg, "enc");
    }

    @Help("CONF <SP> <arg> - Confidentiality protection command.")
    public void ftp_conf(String arg)
            throws CommandExitException
    {
        secure_command(arg, "conf");
    }

    public void secure_command(String answer, String sectype)
            throws CommandExitException
    {
        if (answer == null || answer.length() <= 0) {
            reply("500 Wrong syntax of " + sectype + " command");
            return;
        }

        if (context == null || !context.isEstablished()) {
            reply("503 Security context is not established");
            return;
        }


        byte[] data = Base64.getDecoder().decode(answer);
        try {
            data = context.unwrap(data);
        } catch (IOException e) {
            reply("500 Can not decrypt command: " + e);
            LOGGER.error("GssFtpDoorV1::secure_command: got IOException: {}",
                         e.getMessage());
            return;
        }

        // At least one C-based client sends a zero byte at the end
        // of a secured command. Truncate trailing zeros.
        // Search from the right end of the string for a non-null character.
        int i;
        for (i = data.length; i > 0 && data[i - 1] == 0; i--) {
            //do nothing, just decrement i
        }
        String msg = new String(data, 0, i, UTF8);
        msg = msg.trim();

        if (msg.toLowerCase().startsWith("pass") && msg.length() != 4) {
            _commandLine = sectype.toUpperCase() + "{" + msg.substring(0, 4) + " ...}";
        } else {
            _commandLine = sectype.toUpperCase() + "{" + msg + "}";
        }

        if (msg.equalsIgnoreCase("CCC")) {
            _gReplyType = "clear";
            reply("200 OK");
        } else {
            _gReplyType = sectype;
            ftpcommand(msg);
        }

    }

    @Override
    public void ftp_user(String arg)
    {
        if (arg.equals("")) {
            reply(err("USER", arg));
            return;
        }

        if (context == null || !context.isEstablished()) {
            reply("530 Authentication required");
            return;
        }

        Subject subject = context.getSubject();
        subject.getPrincipals().add(_origin);
        if (!arg.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
            subject.getPrincipals().add(new LoginNamePrincipal(arg));
        }

        try {
            login(subject);
            reply("200 User " + arg + " logged in", this.subject);
        } catch (PermissionDeniedCacheException e) {
            LOGGER.warn("Login denied for {}: {}", context.getPeerName(), e.getMessage());
            println("530 Login denied");
        } catch (CacheException e) {
            LOGGER.error("Login failed for {}: {}", context.getPeerName(), e.getMessage());
            println("530 Login failed: " + e.getMessage());
        }
    }

    // Some clients, even though the user is already logged in via GSS and ADAT,
    // will send a dummy PASS anyway. "Already logged in" is distracting
    // and the "Going to evaluate strong password" message is misleading
    // since nothing is actually done for this command.
    // Example = ubftp client
    @Override
    public void ftp_pass(String arg)
    {
        LOGGER.debug("GssFtpDoorV1::ftp_pass: PASS is a no-op with " +
                     "GSSAPI authentication.");
        if (subject != null) {
            reply(ok("PASS"));
        } else {
            reply("500 Send USER first");
        }
    }
}
