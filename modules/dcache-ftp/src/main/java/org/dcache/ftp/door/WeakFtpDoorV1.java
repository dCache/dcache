package org.dcache.ftp.door;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.util.NetLoggerBuilder;

import static java.util.Objects.requireNonNull;

/**
 *
 * @author  timur
 */
public class WeakFtpDoorV1 extends AbstractFtpDoorV1
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WeakFtpDoorV1.class);

    private final Optional<String> _anonymousUser;
    private final boolean _allowUsernamePassword;
    private final FsPath _anonymousRoot;
    private final boolean _requireAnonPasswordEmail;

    private String _user;
    private boolean _isAnonymous;

    public WeakFtpDoorV1(boolean allowUsernamePassword,
            Optional<String> anonymousUser, FsPath anonymousRoot,
            boolean requireAnonPasswordEmail)
    {
        this("Weak FTP", allowUsernamePassword, anonymousUser, anonymousRoot,
            requireAnonPasswordEmail);
    }

    protected WeakFtpDoorV1(String name, boolean allowUsernamePassword,
            Optional<String> anonymousUser, FsPath anonymousRoot,
            boolean requireAnonPasswordEmail)
    {
        super(name);
        _anonymousUser = requireNonNull(anonymousUser);
        _allowUsernamePassword = allowUsernamePassword;
        _anonymousRoot = anonymousRoot;
        _requireAnonPasswordEmail = requireAnonPasswordEmail;
    }

    @Override
    protected void logSubject(NetLoggerBuilder log, Subject subject)
    {
        if (_isAnonymous) {
            log.add("user.name", _user);
            List<String> emails = Subjects.getEmailAddresses(subject);
            if (!emails.isEmpty()) {
                log.add("user.unverified-email", emails.size() == 1
                        ? emails.get(0) : emails.toString());
            }
        } else {
            log.add("user.name", Subjects.getDisplayName(subject));
        }
    }

    @Override
    protected void secure_reply(CommandRequest request, String answer, String code) {
    }

    @Override
    public void ftp_user(String arg) throws FTPCommandException
    {
        checkFTPCommand(!arg.isEmpty(), 500, "Missing argument");

        _user = arg;
        _isAnonymous = _anonymousUser.isPresent() && _anonymousUser.get().equals(_user);

        if (_isAnonymous) {
            reply("331 Guest login ok, send your email address as password.");
        } else if (_allowUsernamePassword) {
            reply("331 Password required for "+_user+".");
        } else {
            if (_anonymousUser.isPresent()) {
                reply("530 Login with user \"" + _anonymousUser.get()
                        + "\" for anonymous access");
            } else {
                reply("530 USER not supported");
            }
        }
    }

    @Override
    public void ftp_pass(String arg)
    {
        if (_isAnonymous) {
            doAnonymousLogin(arg);
        } else {
            doRegularLogin(arg);
        }
    }

    private void doAnonymousLogin(String email)
    {
        boolean isValidEmail = EmailAddressPrincipal.isValid(email);

        if (!_requireAnonPasswordEmail || isValidEmail) {
            Subject subject = new Subject();

            if (isValidEmail) {
                // REVISIT: this email address trustworthy?
                subject.getPrincipals().add(new EmailAddressPrincipal(email));
            }

            acceptLogin(subject, Collections.emptySet(), Restrictions.readOnly(),
                    _anonymousRoot);
            reply("230 Guest login ok, access restrictions apply.");
        } else {
            LOGGER.debug("Invalid email address as anonymous password: {}", email);
            reply("530 Login failed: email address is not valid");
        }
    }

    private void doRegularLogin(String arg)
    {
        Subject subject = new Subject();
        subject.getPrivateCredentials().add(new PasswordCredential(_user, arg));
        subject.getPrincipals().add(_origin);
        try {
            login(subject);
            reply("230 User " + _user + " logged in");
        } catch (PermissionDeniedCacheException e) {
            LOGGER.warn("Login denied for {}", subject);
            reply("530 Login denied");
        } catch (CacheException e) {
            LOGGER.error("Login failed for {}: {}", subject, e);
            reply("530 Login failed: " + e.getMessage());
        }
    }
}
