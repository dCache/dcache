package org.dcache.ftp.door;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import diskCacheV111.doors.FTPTransactionLog;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.util.CommandExitException;

import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;

/**
 *
 * @author  timur
 */
public class WeakFtpDoorV1 extends AbstractFtpDoorV1
{

    private static final Logger LOGGER = LoggerFactory.getLogger(WeakFtpDoorV1.class);

    @Override
    public void init()
    {
        ftpDoorName = "Weak FTP";
        super.init();
    }

    @Override
    protected void secure_reply(String answer, String code) {
    }

    @Override
    public void ftp_auth(String arg) {
        reply("500 Not Supported");
    }

    @Override
    public void ftp_adat(String arg) {
        reply("500 Not Supported");
    }

    @Override
    public void secure_command(String arg, String sectype) throws CommandExitException {
    }

    private String _user;

    @Override
    public void ftp_user(String arg)
    {
        if (arg.equals("")){
            reply(err("USER",arg));
            return;
        }
        _user = arg;

        reply("331 Password required for "+_user+".");
    }

    @Override
    public void ftp_pass(String arg)
    {
        Subject subject = new Subject();
        subject.getPrivateCredentials().add(new PasswordCredential(_user, arg));
        try {
            login(subject);
        } catch (PermissionDeniedCacheException e) {
            LOGGER.warn("Login denied for {}", subject);
            reply("530 Login denied");
        } catch (CacheException e) {
            LOGGER.error("Login failed for {}: {}", subject, e);
            reply("530 Login failed: " + e.getMessage());
        }

        reply("230 User " + _user + " logged in");
    }

    @Override
    public void startTlog(FTPTransactionLog tlog, String path, String action) {
        if (_subject != null) {
            try {
                String user =
                    Subjects.getUserName(_subject) + "("+Subjects.getUid(_subject) + "." + Subjects.getPrimaryGid(_subject) + ")";
                tlog.begin(user, "weakftp", action, path, _remoteAddress.getAddress());
            }
            catch (Exception e) {
                LOGGER.error("WeakFtpDoor: couldn't start tLog. " +
                        "Ignoring exception: {}", e.getMessage());
            }
        }
    }
}
