package org.dcache.ftp.door;

import com.google.common.base.Throwables;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.cert.CertPathValidatorException;

import diskCacheV111.util.Base64;

import dmg.util.CommandExitException;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;

public abstract class GssFtpDoorV1 extends AbstractFtpDoorV1
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GssFtpDoorV1.class);

    public static final String GLOBUS_URL_COPY_DEFAULT_USER =
        ":globus-mapping:";

    private static final Charset UTF8 = Charset.forName("UTF-8");

    protected GSSName _gssIdentity;
    // GSS general
    protected String _gssFlavor;

    // GSS GSI context and others
    protected GSSContext _serviceContext;

    @Override
    public void init()
    {
        _gssFlavor = "unknown";
        super.init();
    }

    @Override
    protected void secure_reply(String answer, String code) {
        answer = answer+"\r\n";
        byte[] data = answer.getBytes(UTF8);
        MessageProp prop = new MessageProp(0, false);
        try{
            data = _serviceContext.wrap(data, 0, data.length, prop);
        } catch ( GSSException e ) {
            reply("500 Reply encryption error: " + e);
            return;
        }
        println(code + " " + Base64.byteArrayToBase64(data));
    }

    @Override
    public void ftp_auth(String arg) {
        LOGGER.info("GssFtpDoorV1::secure_reply: going to authorize using {}", _gssFlavor);
        if ( !arg.equals("GSSAPI") ) {
            reply("504 Authenticating method not supported");
            return;
        }
        if (_serviceContext != null && _serviceContext.isEstablished()) {
            reply("234 Already authenticated");
            return;
        }

        try {
            _serviceContext = getServiceContext();
        } catch( Exception e ) {
            LOGGER.error(e.toString());
            reply("500 Error: " + e.toString());
            return;
        }
        reply("334 ADAT must follow");
    }

    @Override
    public void ftp_adat(String arg) {
        if ( arg == null || arg.length() <= 0 ) {
            reply("501 ADAT must have data");
            return;
        }

        if ( _serviceContext == null ) {
            reply("503 Send AUTH first");
            return;
        }
        byte[] token = Base64.base64ToByteArray(arg);
        try {
            ChannelBinding cb = new ChannelBinding(_remoteAddress.getAddress(),
            InetAddress.getLocalHost(), null);
        } catch( UnknownHostException e ) {
            reply("500 Can not determine address of local host: " + e);
            return;
        }

        try {
            //_serviceContext.setChannelBinding(cb);
            //debug("GssFtpDoorV1::ftp_adat: CB set");
            token = _serviceContext.acceptSecContext(token, 0, token.length);
            //debug("GssFtpDoorV1::ftp_adat: Token created");
            _gssIdentity = _serviceContext.getSrcName();
            //debug("GssFtpDoorV1::ftp_adat: User principal: " + UserPrincipal);
        } catch (GSSException e) {
            CertPathValidatorException cpve =
                    getFirst(filter(Throwables.getCausalChain(e), CertPathValidatorException.class), null);
            if (cpve != null && cpve.getCertPath() != null && LOGGER.isDebugEnabled()) {
                LOGGER.error("Authentication failed: {} in #{} of {}",
                        e.getMessage(), cpve.getIndex() + 1, cpve.getCertPath());
            } else {
                LOGGER.error("Authentication failed: {}", e.getMessage());
            }
	    LOGGER.trace("Authentication failed", e);
            reply("535 Authentication failed: " + e.getMessage());
            return;
        }
        if (token != null) {
            if (!_serviceContext.isEstablished()) {
                reply("335 ADAT="+Base64.byteArrayToBase64(token));
            }
            else {
                reply("235 ADAT="+Base64.byteArrayToBase64(token));
            }
        }
        else {
            if (!_serviceContext.isEstablished()) {
                reply("335 ADAT=");
            }
            else {
                LOGGER.info("GssFtpDoorV1::ftp_adat: security context established " +
                        "with {}", _gssIdentity);
                reply("235 OK");
            }
        }
    }

    @Override
    public void secure_command(String answer, String sectype)
    throws CommandExitException {
        if ( answer == null || answer.length() <= 0 ) {
            reply("500 Wrong syntax of "+sectype+" command");
            return;
        }

        if ( _serviceContext == null || !_serviceContext.isEstablished()) {
            reply("503 Security context is not established");
            return;
        }


        byte[] data = Base64.base64ToByteArray(answer);
        MessageProp prop = new MessageProp(0, false);
        try {
            data = _serviceContext.unwrap(data, 0, data.length, prop);
        } catch( GSSException e ) {
            reply("500 Can not decrypt command: " + e);
            LOGGER.error("GssFtpDoorV1::secure_command: got GSSException: {}",
                    e.getMessage());
            return;
        }

        // At least one C-based client sends a zero byte at the end
        // of a secured command. Truncate trailing zeros.
        // Search from the right end of the string for a non-null character.
        int i;
        for(i = data.length;i > 0 && data[i-1] == 0 ;i--) {
            //do nothing, just decrement i
        }
        String msg = new String(data, 0, i, UTF8);
        msg = msg.trim();

        if (msg.toLowerCase().startsWith("pass") && msg.length() != 4) {
            _currentCmdLine = sectype.toUpperCase() + "{" + msg.substring(0, 4) + " ...}";
        } else {
            _currentCmdLine = sectype.toUpperCase() + "{" + msg + "}";
        }

        if ( msg.equalsIgnoreCase("CCC") ) {
            _gReplyType = "clear";
            reply("200 OK");
        }
        else {
            _gReplyType = sectype;
            ftpcommand(msg);
        }

    }

    @Override
    protected String getUser()
    {
        return (_gssIdentity == null) ? null : _gssIdentity.toString();
    }

    // Some clients, even though the user is already logged in via GSS and ADAT,
    // will send a dummy PASS anyway. "Already logged in" is distracting
    // and the "Going to evaluate strong password" message is misleading
    // since nothing is actually done for this command.
    // Example = ubftp client
    @Override
    public void ftp_pass(String arg) {
        LOGGER.debug("GssFtpDoorV1::ftp_pass: PASS is a no-op with " +
                "GSSAPI authentication.");
        if (_subject != null) {
            reply(ok("PASS"));
        } else {
            reply("500 Send USER first");
        }
    }


    /**
     * The concrete implementation of this method returns the GSSContext
     * specific to the particular security mechanism.
     */
    protected abstract GSSContext getServiceContext() throws GSSException;
}
