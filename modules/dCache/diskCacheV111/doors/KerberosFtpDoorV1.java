// $Id: KerberosFtpDoorV1.java,v 1.12 2007-10-26 08:19:34 behrmann Exp $
// $Log: not supported by cvs2svn $
// Revision 1.11  2007/10/25 22:51:10  timur
// make doors/KerberosFtpDoorV1.java compile after recent Gerd's changes
//
// Revision 1.10  2005/05/19 05:55:43  timur
// added support for monitoring door state via dcache pages
//
// Revision 1.9  2004/09/09 20:27:37  timur
// made ftp transaction logging optional
//
// Revision 1.8  2004/09/09 18:39:40  timur
// added the uid,gid to the user names in FTP logs
//
// Revision 1.7  2004/09/08 21:25:43  timur
// remote gsiftp transfer manager will now use ftp logger too, fixed ftp door logging problem
//
// Revision 1.6  2004/08/19 18:22:28  timur
// gridftp door gives pool a host name instead of address, reformated code
//
// Revision 1.5  2003/09/25 16:52:05  cvs
// use globus java cog kit gsi gss library instead of gsint
//
// Revision 1.4  2003/09/02 21:06:20  cvs
// removed logging of security info by kftp and gsiftp doors, changest in srm
//
// Revision 1.3  2003/05/12 19:26:19  cvs
// create worker threads from sublclasses
//
// Revision 1.2  2003/05/07 17:44:24  cvs
// new ftp doors are ready
//
// Revision 1.1  2003/05/06 22:10:48  cvs
// new ftp door classes structure
//
/*
 * KerberosFtpDoorV1.java
 *
 * Created on May 6, 2003, 3:05 PM
 */

package diskCacheV111.doors;

//java util
import java.util.StringTokenizer;
import java.util.Properties;

//java net
import java.net.InetAddress;
import java.net.UnknownHostException;

//cells
import dmg.util.StreamEngine;
import dmg.util.Args;

//jgss
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSName;


/**
 *
 * @author  timur
 */
public class KerberosFtpDoorV1 extends GssFtpDoorV1 {

    private String MyPrincipalStr;
// TODO: the str1 variable looks like it was once some sort of debug aid
//       but it seems like it should be removed...
    private String str1;
    private String[] KDCList;

    /** Creates a new instance of KerberosFtpDoorV1 */
    public KerberosFtpDoorV1(String name, StreamEngine engine, Args args) throws Exception{
        super(name,engine,args);
        if( ( MyPrincipalStr = args.getOpt("svc-principal") ) == null ){
            String problem = "KerberosFTPDoorV1: -svc-principal not specified";
            error(problem);
            start();
            kill();
            throw new IllegalArgumentException(problem);
        }
        info("KerberosFTPDoorV1: initializing kerberos ftp door service. " +
             "Principal is '" + MyPrincipalStr + "'");
        _GSSFlavor = "k5";
        str1 = MyPrincipalStr;
        debug("KerberosFTPDoorV1: str1 is " + str1);

        String kdclist;
        if( ( kdclist = args.getOpt("kdc-list") ) != null ) {
            StringTokenizer tokens = new StringTokenizer(kdclist, ",");
            int n = tokens.countTokens();
            KDCList = new String[n];
            for( int i = 0; i < n; i++ ) {
                KDCList[i] = tokens.nextToken();
                info("KerberosFTPDoorV1: kdc[" + i + "] = " + KDCList[i]);
            }
        }
        ftpDoorName="Kerberos FTP";
        _workerThread = getNucleus().newThread(this);
        _workerThread.start();
        useInterpreter(true);
        doInit();
    }

    public void startTlog(String path, String action) {
        if (_tLog == null) {
            return;
        }
        try {
            String user_string = _user;
            if (_pwdRecord != null) {
                user_string += "("+_pwdRecord.UID+"."+_pwdRecord.GID+")";
            }
            _tLog.begin(user_string, "krbftp", action, path,
                        _engine.getInetAddress());
        }
        catch (Exception e) {
            error("KerberosFTPDoorV1::startTlog: couldn't start tLog. " +
                  "Ignoring exception: " + e.getMessage());
        }
    }

    protected GSSContext getServiceContext() throws GSSException {
        Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
        Oid krb5PrincipalNameType = new Oid("1.2.840.113554.1.2.2.1");
        int nretry = 10;
        GSSException error = null;
        Properties sysp = System.getProperties();
        GSSCredential MyCredential = null;

        GSSManager _GManager = GSSManager.getInstance();
        debug("KerberosFTPDoorV1::getServiceContext: calling " +
             "_GManager.createName(\"" + MyPrincipalStr + "\", null)");
        debug("KerberosFTPDoorV1::getServiceContext str1 is " + str1);
        GSSName MyPrincipal = _GManager.createName(MyPrincipalStr, null);
        info("KerberosFTPDoorV1::getServiceContext: principal=\"" +
             MyPrincipal + "\"");

        while( MyCredential == null && nretry-- > 0 ) {
            if( KDCList != null && KDCList.length > 0 ) {
                String kdc = KDCList[nretry % KDCList.length];
                sysp.put("java.security.krb5.kdc", kdc);
            }

            try {
                MyCredential = _GManager.createCredential(MyPrincipal,
                GSSCredential.DEFAULT_LIFETIME,
                krb5Mechanism,
                GSSCredential.ACCEPT_ONLY);
            }
            catch( GSSException e ) {
                debug("KerberosFTPDoorV1::getServiceContext: got exception " +
                      " while looking up credential: " + e.getMessage());
                error = e;
            }
        }
        if( MyCredential == null ) {
            throw error;
        }
        info("KerberosFTPDoorV1::getServiceContext: credential=\"" +
             MyCredential + "\"");
        GSSContext context = _GManager.createContext(MyCredential);

        try {
            ChannelBinding cb = new ChannelBinding(_engine.getInetAddress(),
                                                   InetAddress.getLocalHost(),
                                                   null);
            context.setChannelBinding(cb);
        }
        catch( UnknownHostException e ) {
            String errmsg = "KerberosFTPDoorV1::getServiceContext: can't " +
                            "bind channel to localhost:" + e.getMessage();
            error(errmsg);
            throw new GSSException(GSSException.NO_CRED, 0, errmsg);
        }

        return context;
    }
}
