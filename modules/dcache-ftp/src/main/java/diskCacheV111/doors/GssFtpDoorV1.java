package diskCacheV111.doors;

import com.google.common.base.Throwables;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.cert.CertPathValidatorException;
import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.Base64;

import dmg.util.Args;
import dmg.util.CommandExitException;
import dmg.util.StreamEngine;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public abstract class GssFtpDoorV1 extends AbstractFtpDoorV1
{
    private static final GssCommandContext SECURE_COMMAND_CONTEXT = new GssCommandContext();
    public static final String GLOBUS_URL_COPY_DEFAULT_USER =
        ":globus-mapping:";

    private static final Charset UTF8 = Charset.forName("UTF-8");

    protected GSSName _gssIdentity;
    // GSS general
    protected String _gssFlavor;

    // GSS GSI context and others
    protected GSSContext _serviceContext;

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

    /** Creates a new instance of GsiFtpDoorV1 */
    public GssFtpDoorV1(String name, StreamEngine engine, Args args)
    {
        super(name,engine,args);

        visitFtpCommands(new CommandMethodVisitor() {
            @Override
            public void acceptCommand(Method method, String command) {
                Plaintext plaintext = method.getAnnotation(Plaintext.class);
                if (plaintext != null) {
                    _plaintextCommands.add(command);
                }
            }
        });
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();
        _gssFlavor = "unknown";
    }

    @Override
    protected void secure_reply(String answer, String code) {
        answer = answer+"\r\n";
        byte[] data = answer.getBytes(UTF8);
        MessageProp prop = new MessageProp(0, false);
        try{
            data = _serviceContext.wrap(data, 0, data.length, prop);
        } catch ( GSSException e ) {
            println("500 Reply encryption error: " + e);
            return;
        }
        println(code + " " + Base64.byteArrayToBase64(data));
    }

    @Override
    @Plaintext
    public void ac_auth(String arg) {
        info("GssFtpDoorV1::secure_reply: going to authorize using " + _gssFlavor);
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
            error(e.toString());
            reply("500 Error: " + e.toString());
            return;
        }
        reply("334 ADAT must follow");
    }

    @Override
    @Plaintext
    public void ac_adat(String arg) {
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
            ChannelBinding cb = new ChannelBinding(_engine.getInetAddress(),
            InetAddress.getLocalHost(), null);
        } catch( UnknownHostException e ) {
            reply("500 Can not determine address of local host: " + e);
            return;
        }

        try {
            enableInterrupt();
            //_serviceContext.setChannelBinding(cb);
            //debug("GssFtpDoorV1::ac_adat: CB set");
            token = _serviceContext.acceptSecContext(token, 0, token.length);
            //debug("GssFtpDoorV1::ac_adat: Token created");
            _gssIdentity = _serviceContext.getSrcName();
            //debug("GssFtpDoorV1::ac_adat: User principal: " + UserPrincipal);
        } catch (InterruptedException e) {
            reply("421 Service unavailable");
            return;
        } catch (GSSException e) {
            CertPathValidatorException cpve =
                    getFirst(filter(Throwables.getCausalChain(e), CertPathValidatorException.class), null);
            if (cpve != null && cpve.getCertPath() != null && _logger.isDebugEnabled()) {
                _logger.error("Authentication failed: {} in #{} of {}",
                        e.getMessage(), cpve.getIndex() + 1, cpve.getCertPath());
            } else {
                _logger.error("Authentication failed: {}", e.getMessage());
            }
	    _logger.trace("Authentication failed", e);
            reply("535 Authentication failed: " + e.getMessage());
            return;
        } finally {
            disableInterrupt();
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
                info("GssFtpDoorV1::ac_adat: security context established " +
                     "with " + _gssIdentity);
                reply("235 OK");
            }
        }
    }

    public void ac_ccc(String arg)
    {
        // We should never received this, only through MIC, ENC or CONF,
        // in which case it will be intercepted by secure_command()
        reply("533 CCC must be protected");
    }

    @Plaintext
    public void ac_mic(String arg)
        throws CommandExitException
    {
        secure_command(arg, "mic");
    }

    @Plaintext
    public void ac_enc(String arg)
        throws CommandExitException
    {
        secure_command(arg, "enc");
    }

    @Plaintext
    public void ac_conf(String arg)
        throws CommandExitException
    {
        secure_command(arg, "conf");
    }

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
            error("GssFtpDoorV1::secure_command: got GSSException: " +
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

        if ( msg.equalsIgnoreCase("CCC") ) {
            _gReplyType = "clear";
            _hasControlPortCleared = true;
            reply("200 OK");
        }
        else {
            _gReplyType = sectype;
            ftpcommand(msg, SECURE_COMMAND_CONTEXT);
        }

    }

    @Override
    protected boolean isCommandAllowed(String command, Object commandContext)
    {
        boolean isSecureCommand = commandContext == SECURE_COMMAND_CONTEXT;

        if (!_hasControlPortCleared && !isSecureCommand &&
                !_plaintextCommands.contains(command)) {
            reply("530 Command must be wrapped in MIC, ENC or CONF", false);
            return false;
        }

        return super.isCommandAllowed(command, commandContext);
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
    public void ac_pass(String arg) {
        debug("GssFtpDoorV1::ac_pass: PASS is a no-op with " +
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
