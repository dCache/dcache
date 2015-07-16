package org.dcache.ftp.door;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import diskCacheV111.doors.FTPTransactionLog;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;

/**
 *
 * @author  timur
 */
public class WeakFtpDoorV1 extends AbstractFtpDoorV1
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WeakFtpDoorV1.class);

    public WeakFtpDoorV1()
    {
        super("Weak FTP", "weakftp");
    }

    @Override
    protected void secure_reply(String answer, String code) {
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
            reply("230 User " + _user + " logged in", ImmutableMap.of(
                            "user.name", Subjects.getDisplayName(_subject)));
        } catch (PermissionDeniedCacheException e) {
            LOGGER.warn("Login denied for {}", subject);
            reply("530 Login denied");
        } catch (CacheException e) {
            LOGGER.error("Login failed for {}: {}", subject, e);
            reply("530 Login failed: " + e.getMessage());
        }
    }
}
