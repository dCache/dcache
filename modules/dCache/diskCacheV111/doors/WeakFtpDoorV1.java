// $Id: WeakFtpDoorV1.java,v 1.11 2007-10-29 13:29:24 behrmann Exp $
// $Log: not supported by cvs2svn $
// Revision 1.10  2007/10/25 20:02:42  behrmann
// Made all fields conform to normal dCache naming policy.
//
// Revision 1.9  2005/05/19 05:55:43  timur
// added support for monitoring door state via dcache pages
//
// Revision 1.8  2004/09/09 20:27:37  timur
// made ftp transaction logging optional
//
// Revision 1.7  2004/09/09 18:39:40  timur
// added the uid,gid to the user names in FTP logs
//
// Revision 1.6  2004/09/08 21:25:43  timur
// remote gsiftp transfer manager will now use ftp logger too, fixed ftp door logging problem
//
// Revision 1.5  2004/08/19 18:22:28  timur
// gridftp door gives pool a host name instead of address, reformated code
//
// Revision 1.4  2003/09/25 16:52:05  cvs
// use globus java cog kit gsi gss library instead of gsint
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
 * WeakFtpDoorV1.java
 *
 * Created on May 6, 2003, 3:07 PM
 */

package diskCacheV111.doors;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;
import diskCacheV111.cells.*;


import dmg.cells.nucleus.*;
import dmg.cells.network.*;
import dmg.util.*;

import java.util.*;
import java.io.*;
import java.net.*;
import java.lang.reflect.*;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.Password;
import org.dcache.auth.Subjects;
import org.dcache.auth.KauthFileLoginStrategy;
import org.ietf.jgss.*;
import java.lang.Thread;
import java.util.regex.*;
import java.util.concurrent.ExecutionException;

import javax.security.auth.Subject;

/**
 *
 * @author  timur
 */
public class WeakFtpDoorV1 extends AbstractFtpDoorV1 {

    /** Creates a new instance of WeakFtpDoorV1 */
    public WeakFtpDoorV1(String name, StreamEngine engine, Args args)
        throws InterruptedException, ExecutionException
    {
        super(name,engine,args);
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();
        ftpDoorName = "Weak FTP";
        _loginStrategy =
            new KauthFileLoginStrategy(new File(_kpwdFilePath));
    }

    protected void secure_reply(String answer, String code) {
    }

    public void ac_auth(String arg) {
        reply("500 Not Supported");
    }

    public void ac_adat(String arg) {
        reply("500 Not Supported");
    }

    public void secure_command(String arg, String sectype) throws dmg.util.CommandExitException {
    }

    private String _user;

    public void ac_user(String arg)
    {
        if (arg.equals("")){
            println(err("USER",arg));
            return;
        }
        _user = arg;

        println("331 Password required for "+_user+".");
    }

    public void ac_pass(String arg)
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(new LoginNamePrincipal(_user));
        subject.getPrivateCredentials().add(new Password(arg));
        try {
            login(subject);
        } catch (PermissionDeniedCacheException e) {
            warn("Login denied for " + subject);
            println("530 Login incorrect");
        } catch (CacheException e) {
            error("Login failed for " + subject + ": " + e);
            println("530 Login failed: " + e.getMessage());
        }

        println("230 User " + _user + " logged in");
    }

    @Override
    public void startTlog(FTPTransactionLog tlog, String path, String action) {
        if (_subject != null) {
            try {
                String user =
                    Subjects.getUserName(_subject) + "("+Subjects.getUid(_subject) + "." + Subjects.getPrimaryGid(_subject) + ")";
                tlog.begin(user, "weakftp", action, path,
                           _engine.getInetAddress());
            }
            catch (Exception e) {
                error("WeakFtpDoor: couldn't start tLog. " +
                      "Ignoring exception: " + e.getMessage());
            }
        }
    }
}
