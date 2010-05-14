/*
 * GssFtpDoorV1.java
 *
 * Created on Sep 24, 2003, 9:53 AM
 */

package diskCacheV111.doors;

//cells
import dmg.cells.nucleus.CellVersion;
import dmg.util.StreamEngine;
import dmg.util.Args;

//dcache
import diskCacheV111.util.Base64;
import org.dcache.auth.*;
import gplazma.authz.AuthorizationException;
import diskCacheV111.services.acl.GrantAllPermissionHandler;

//java
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.File;
import java.util.Iterator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.security.auth.Subject;

//jgss
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
/**
 *
 * @author  timur
 */
public abstract class GssFtpDoorV1 extends AbstractFtpDoorV1
{
    public static final String GLOBUS_URL_COPY_DEFAULT_USER =
        ":globus-mapping:";

    protected GSSName _gssIdentity;
    // GSS general
    protected String _gssFlavor;

    // GSS GSI context and others
    protected GSSContext _serviceContext;

    // For multiple attribute support
    //protected AuthenticationMessage authmessage;

    /** Creates a new instance of GsiFtpDoorV1 */
    public GssFtpDoorV1(String name, StreamEngine engine, Args args)
        throws InterruptedException, ExecutionException
    {
        super(name,engine,args);
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();
        _gssFlavor = "unknown";

        if (_use_gplazmaAuthzCell || _use_gplazmaAuthzModule) {
            _loginStrategy =
                new GplazmaLoginStrategy(new AuthzQueryHelper(this));
        } else {
            _loginStrategy =
                new KauthFileLoginStrategy(new File(_kpwdFilePath));
        }
    }

    protected void secure_reply(String answer, String code) {
        answer = answer+"\r\n";
        byte[] data = answer.getBytes();
        MessageProp prop = new MessageProp(0, false);
        try{
            data = _serviceContext.wrap(data, 0, data.length, prop);
        } catch ( GSSException e ) {
            println("500 Reply encryption error: " + e);
            return;
        }
        println(code + " " + Base64.byteArrayToBase64(data));
    }

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
        ChannelBinding cb;
        try {
            cb = new ChannelBinding(_engine.getInetAddress(),
            InetAddress.getLocalHost(), null);
        } catch( UnknownHostException e ) {
            reply("500 Can not determine address of local host: " + e);
            return;
        }

        try {
            //_serviceContext.setChannelBinding(cb);
            //debug("GssFtpDoorV1::ac_adat: CB set");
            token = _serviceContext.acceptSecContext(token, 0, token.length);
            //debug("GssFtpDoorV1::ac_adat: Token created");
            _gssIdentity = _serviceContext.getSrcName();
            //debug("GssFtpDoorV1::ac_adat: User principal: " + UserPrincipal);
        } catch( Exception e ) {
            _logger.error("GssFtpDoorV1::ac_adat: got service context exception", e);
            reply("535 Authentication failed: " + e);
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
                info("GssFtpDoorV1::ac_adat: security context established " +
                     "with " + _gssIdentity);
                reply("235 OK");
            }
        }
    }

    public void secure_command(String answer, String sectype)
    throws dmg.util.CommandExitException {
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
        String msg = new String(data, 0, i);
        msg = msg.trim();

        if ( msg.equalsIgnoreCase("CCC") ) {
            _gReplyType = "clear";
            reply("200 OK");
        }
        else {
            _gReplyType = sectype;
            ftpcommand(msg);
        }

    }

    public static CellVersion getStaticCellVersion() {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision: 1.18 $" );
    }

    @Override
    protected String getUser()
    {
        return _gssIdentity.toString();
    }

    protected void resetPwdRecord()
    {
    }

    protected boolean setNextPwdRecord()
    {
        return false;
    }

    // Some clients, even though the user is already logged in via GSS and ADAT,
    // will send a dummy PASS anyway. "Already logged in" is distracting
    // and the "Going to evaluate strong password" message is misleading
    // since nothing is actually done for this command.
    // Example = ubftp client
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
