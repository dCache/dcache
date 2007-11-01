// $Id: GssFtpDoorV1.java,v 1.10.2.6 2007-02-28 23:10:01 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.10.2.5  2006/11/30 21:51:11  tdh
// Pass cell reference to AuthorizationService for logging purposes.
//
// Revision 1.10.2.4  2006/11/13 19:40:00  tigran
// changes from HEAD:
// UserMetaDataProvider_gPlazma and GssFtpDoorV1 passing cell to gPlazma constructor to keep say and esay under control
// AuthorizationService closes delegation socket (CLOSE_WAIT)
// SocketAdapter code cleanup
// EBlockReceiverNio fixed pool looping
//
// Revision 1.13  2006/11/12 15:53:18  tigran
// pass cell to gPlazma
// if cell is not given, then System.out.println is used
//
// Revision 1.12  2006/08/07 20:16:42  tdh
// Do not reply 530 if gPlazma cell fails but direct call to gplazma modules are still to be made.
//
// Revision 1.11  2006/07/25 16:05:40  tdh
// Make message to gPlazma cell independent of gPlazma domain.
//
// Revision 1.10  2006/07/03 19:56:50  tdh
// Added code to throw and/or catch AuthenticationServiceExceptions from GPLAZMA cell.
//
// Revision 1.9  2006/06/29 20:25:30  tdh
// Changed hard-coded path of gplazma cell to gPlazma@gPlazmaDomain.
//
// Revision 1.8  2006/06/13 17:15:09  tdh
// Changed logic of ac_user to use gplazma cell for authentification if specified.
//
// Revision 1.7  2005/11/22 10:59:30  patrick
// Versioning enabled.
//
// Revision 1.6  2005/09/14 17:18:20  kennedy
// Do not say <user already logged in> when dummy PASS sent by client
//
// Revision 1.5  2005/09/14 14:12:15  tigran
// fixed copy/paste error
// added _dnUser for GSS/GSI
//
// Revision 1.4  2005/05/20 16:51:32  timur
// adding optional usage of vo authorization module
//
// Revision 1.3  2004/09/08 21:25:43  timur
// remote gsiftp transfer manager will now use ftp logger too, fixed ftp door logging problem
//
// Revision 1.2  2004/08/19 18:22:28  timur
// gridftp door gives pool a host name instead of address, reformated code
//
// Revision 1.1  2003/09/25 16:52:42  cvs
// use globus java cog kit gsi gss library instead of gsint
//
//
// Revision 1.1  2003/05/06 22:10:48  cvs
// new ftp door classes structure
//
/*
 * GssFtpDoorV1.java
 *
 * Created on Sep 24, 2003, 9:53 AM
 */

package diskCacheV111.doors;

//cells
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.CellPath;
import dmg.util.StreamEngine;
import dmg.util.Args;

//dcache
import diskCacheV111.util.Base64;
import diskCacheV111.util.KAuthFile;
import diskCacheV111.util.UserAuthRecord;

//java
import java.net.InetAddress;
import java.net.UnknownHostException;

//jgss
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;

import diskCacheV111.services.authorization.AuthorizationService;


/**
 *
 * @author  timur
 */
public abstract class GssFtpDoorV1 extends AbstractFtpDoorV1 {
    
    public static final String GLOBUS_URL_COPY_DEFAULT_USER =
    ":globus-mapping:";
    
    protected GSSName GSSIdentity;
    //GSS general
    protected String _GSSFlavor = "unknown";
    
    // GSS GSI context and others
    protected GSSContext serviceContext;
    
    /** Creates a new instance of GsiFtpDoorV1 */
    public GssFtpDoorV1(String name, StreamEngine engine, Args args) throws Exception{
        super(name,engine,args);
    }
    
    
    protected void secure_reply(String answer, String code) {
        answer = answer+"\r\n";
        byte[] data = answer.getBytes();
        MessageProp prop = new MessageProp(0, false);
        try{
            data = serviceContext.wrap(data, 0, data.length, prop);
        }
        catch ( GSSException e ) {
            println("500 Reply encrypton error: "+e);
            return;
        }
        println(code + " " + Base64.byteArrayToBase64(data));
    }
    
    public void ac_auth(String arg)     {
        say("Going to authenticate "+_GSSFlavor);
        if ( !arg.equals("GSSAPI") ) {
            reply("504 Authenticating method not supported");
            return;
        }
        if( serviceContext != null && serviceContext.isEstablished()) {
            reply("234 Already authenticated");
            return;
        }
        
        try {
            serviceContext = getServiceContext();
        }
        catch( Exception e ) {
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
        
        if ( serviceContext == null ) {
            reply("503 Send AUTH first");
            return;
        }
        byte[] token = Base64.base64ToByteArray(arg);
        ChannelBinding cb;
        try {
            cb = new ChannelBinding(_engine.getInetAddress(),
            InetAddress.getLocalHost(), null);
            //say("Local address: " + InetAddress.getLocalHost());
            //say("Client address: " + _engine.getInetAddress());
        }
        catch( UnknownHostException e )
        {	reply("500 Can not determine address of local host: " + e);
                return;
        }
        
        try {
            //            serviceContext.setChannelBinding(cb);
            //say("CB set");
            token = serviceContext.acceptSecContext(token, 0, token.length);
            //say("Token created");
            GSSIdentity = serviceContext.getSrcName();
            //say("User principal: "+UserPrincipal);
        }
        catch( Exception e ) {
            e.printStackTrace();
            reply("535 Authentication failed: " + e);
            return;
        }
        if (token != null) {
            if(!serviceContext.isEstablished()) {
                reply("335 ADAT="+Base64.byteArrayToBase64(token));
            }
            else {
                reply("235 ADAT="+Base64.byteArrayToBase64(token));
            }
        }
        else {
            if(!serviceContext.isEstablished()) {
                reply("335 ADAT=");
            }
            else {
                say(" scurity context is established with "+GSSIdentity);
                reply("235 OK");
            }
        }
    }
    
    
    public void secure_command(String answer, String sectype)
    throws dmg.util.CommandExitException {
        if( answer == null || answer.length() <= 0 ) {
            reply("500 Wrong syntax of "+sectype+" command");
            return;
        }
        
        if( serviceContext == null || !serviceContext.isEstablished()){
            reply("503 Security context is not established");
            return;
        }
        
        
        byte[] data = Base64.base64ToByteArray(answer);
        MessageProp prop = new MessageProp(0, false);
        try { data = serviceContext.unwrap(data, 0, data.length, prop); }
        catch( GSSException e )
        {	reply("500 Can not decrypt command: " + e);
                e.printStackTrace();
                return;
        }
        
        // At least one C-based client sends zero byte at the end
        // of secured command. Truncate trailing zeros.
        int i;
        for(i = data.length;i > 0 && data[i-1] == 0 ;i--) {
            //do nothing, just decrement i
        }
        String msg = new String(data, 0, i);
        msg = msg.trim();
        say("Secure command: <" + msg + ">");
        
        if( msg.equalsIgnoreCase("CCC") ) {
            GReplyType = "clear";
            reply("200 OK");
        }
        else {
            GReplyType = sectype;
            ftpcommand(msg);
        }
        
    }
    
    public static CellVersion getStaticCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.10.2.6 $" ); }


    public void ac_user(String arg) {

      KAuthFile authf;
      AuthorizationService authServ=null;

      _PwdRecord = null;
      _user = null;
      say("ac_user <"+arg+">");
      if (arg.equals("")){
        reply(err("USER",arg));
        return;
      }
        
      if (serviceContext == null || !serviceContext.isEstablished()){
        reply("530 Authentication required");
        return;
      }
        
      _user = arg;
      _dnUser = GSSIdentity.toString();
      if (!use_gplazmaAuthzCell && !use_gplazmaAuthzModule) {
        try {
          authf = new KAuthFile(_KPwdFilePath);
        }
        catch( Exception e ) {
          reply("530 User authentication file not found: " + e);
          return;
        }
        
        if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
          say("special case , user is "+_user);
          _user = authf.getIdMapping(GSSIdentity.toString() );
          if(_user == null) {
            reply("530 User Name for GSI Identity" +
            GSSIdentity.toString() + " not found.");
            return;
          }
        }
        
        _PwdRecord = authf.getUserRecord(_user);
        if( _PwdRecord == null ) {
          reply("530 User " + _user + " not found.");
          return;
        }
        
        say("Looking up: " + GSSIdentity.toString());
        if( !((UserAuthRecord)_PwdRecord).hasSecureIdentity(GSSIdentity.toString()) ) {
          _PwdRecord = null;
          reply("530 Permission denied");
          return;
        }
      }

      if (use_gplazmaAuthzCell) {
        try {
          authServ = new AuthorizationService(this);
          //_PwdRecord = authServ.authenticate(serviceContext, new CellPath("gPlazma"), this);
          _PwdRecord = authServ.authenticate(serviceContext, _user, new CellPath("gPlazma"), this);
        } catch( Exception e ) {
          if(!use_gplazmaAuthzModule) {
            reply("530 Authorization Service failed: " + e);
          }
          say("Authorization through gPlazma cell failed " + e);
          _PwdRecord = null;
        }
      }

      
      if (_PwdRecord==null && use_gplazmaAuthzModule) {
        try {
          authServ = new AuthorizationService(gplazmaPolicyFilePath, this);
        } catch( Exception e ) {
          reply("530 Authorization Service failed to initialize: " + e);
          return;
        }
        if(_user.equals(GLOBUS_URL_COPY_DEFAULT_USER)) {
          say("special case, user is "+_user);
          try {
            _PwdRecord = authServ.authorize(serviceContext, null, null, null);
            _user = _PwdRecord.Username;
          } catch ( Exception e ) {
            reply("530 User Authorization record failed to be retrieved: " + e);
            return;
          }
        } else {
          try {
            _PwdRecord = authServ.authorize(serviceContext, _user, null, null);
          } catch ( Exception e ) {
            reply("530 User Authorization record failed to be retrieved: " + e);
            return;
          }
        }
      }

      if(_PwdRecord == null) {
        reply("530 Permission denied");
        return;
      }

      _CurDirV = _PwdRecord.Home;
      _PathRoot = _PwdRecord.Root;
      if ( _PathRoot == null || _PathRoot.length() == 0 ) _PathRoot = "/";
      _PnfsFsRoot = _PwdRecord.FsRoot;
      if ( _PnfsFsRoot == null || _PnfsFsRoot.length() == 0 ) _PnfsFsRoot = _PathRoot;
        
      reply("200 User "+_user+" logged in");
    }
    
// Some clients, even though the user is already logged in via GSS and ADAT
//	will send a dummy PASS anyway. "Already logged in" is distracting
//	and the "Going to evaluate strong password" message is misleading
//	since nothing is actually done for this command.
//      Example = ubftp client
    public void ac_pass(String arg) {
        say( "PASS is a no-op with GSSAPI authentication.");
        if( _PwdRecord != null || GSSIdentity != null ) {
            reply(ok("PASS"));
            return;
        }
        else {
            reply("500 Send USER first");
            return;
        }
    }
    
    
    /**
     * the concrete implementation of this method returns the GSSContext
     * specific to the particular security mechanism
     */
    protected abstract GSSContext getServiceContext() throws GSSException;
}
