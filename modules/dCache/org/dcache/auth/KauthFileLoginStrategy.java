package org.dcache.auth;

import java.util.Set;
import java.util.HashSet;
import java.io.File;
import java.io.IOException;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.globus.gsi.jaas.GlobusPrincipal;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.auth.attributes.ReadOnly;

/**
 * A LoginStrategy that wraps KAuthFile.
 *
 * Supports login for Subjects with
 *
 *   - KeberosPrincipal and optional UserNamePrincipal
 *   - GlobusPrincipal and optional UserNamePrincipal
 *   - UserNamePrincipal and Password as a private credential.
 */
public class KauthFileLoginStrategy implements LoginStrategy
{
    private final File _file;

    public KauthFileLoginStrategy(File file)
    {
        if (!file.canRead()) {
            throw new IllegalArgumentException("File not found: " + file);
        }

        _file = file;
    }

    public LoginReply login(Subject subject) throws CacheException
    {
        String user = Subjects.getUserName(subject);

        for (KerberosPrincipal principal: subject.getPrincipals(KerberosPrincipal.class)) {
            return loginByUserNameAndId(user, principal.getName());
        }

        for (GlobusPrincipal principal: subject.getPrincipals(GlobusPrincipal.class)) {
            return loginByUserNameAndId(user, principal.getName());
        }

        return loginByUserName(subject);
    }

    private KAuthFile loadKauthFile()
        throws CacheException
    {
        try {
            return new KAuthFile(_file.getPath());
        } catch (IOException e) {
            throw new PermissionDeniedCacheException("Password file not found");
        }
    }

    private LoginReply loginByUserNameAndId(String user, String id)
        throws CacheException
    {
        KAuthFile kauth = loadKauthFile();

        if (user == null) {
            user = kauth.getIdMapping(id);
            if (user == null) {
                throw new PermissionDeniedCacheException("Access denied");
            }
        }

        UserAuthRecord record = kauth.getUserRecord(user);
        if (record == null || !record.hasSecureIdentity(id)) {
            throw new PermissionDeniedCacheException("Access denied");
        }

        Subject subject;
        if (record.isAnonymous()) {
            subject = Subjects.NOBODY;
        } else {
            subject = Subjects.getSubject(record);
        }
        return new LoginReply(subject, toLoginAttributes(record));
    }

    private LoginReply loginByUserName(Subject subject)
        throws CacheException
    {
        String user = Subjects.getUserName(subject);
        if (user == null) {
            throw new IllegalArgumentException("Subject is not supported by KAuthFileLoginStrategy");
        }

        KAuthFile kauth = loadKauthFile();

        UserPwdRecord record = kauth.getUserPwdRecord(user);
        if (record == null || record.isDisabled()) {
            throw new PermissionDeniedCacheException("Access denied");
        }

        if (!record.isAnonymous() && !isPasswordCorrect(subject, record)) {
            throw new PermissionDeniedCacheException("Access denied");
        }

        if (record.isAnonymous()) {
            subject = Subjects.NOBODY;
        } else {
            subject = Subjects.getSubject(record, true);
        }
        return new LoginReply(subject, toLoginAttributes(record));
    }

    private Set<LoginAttribute> toLoginAttributes(UserAuthBase record)
    {
        Set<LoginAttribute> attributes = new HashSet<LoginAttribute>();
        attributes.add(new HomeDirectory(record.Home));
        attributes.add(new RootDirectory(record.Root));
        attributes.add(new ReadOnly(record.ReadOnly));
        return attributes;
    }

    private boolean isPasswordCorrect(Subject subject, UserPwdRecord record)
    {
        for (Password password: subject.getPrivateCredentials(Password.class)) {
            if (record.passwordIsValid(password.getPassword())) {
                return true;
            }
        }
        return false;
    }
}