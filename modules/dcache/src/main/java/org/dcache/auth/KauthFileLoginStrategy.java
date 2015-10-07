package org.dcache.auth;

import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.RootDirectory;

/**
 * A LoginStrategy that wraps KAuthFile.
 *
 * Supports login for Subjects with
 *
 *   - KeberosPrincipal and optional LoginNamePrincipal
 *   - GlobusPrincipal and optional LoginNamePrincipal
 *   - PasswordCredential
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

    @Override
    public LoginReply login(Subject subject) throws CacheException
    {
        String user = Subjects.getLoginName(subject);

        for (KerberosPrincipal principal: subject.getPrincipals(KerberosPrincipal.class)) {
            return loginByUserNameAndId(user, principal.getName());
        }

        for (GlobusPrincipal principal: subject.getPrincipals(GlobusPrincipal.class)) {
            return loginByUserNameAndId(user, principal.getName());
        }

        return loginWithPassword(subject);
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

    private LoginReply loginWithPassword(Subject subject)
        throws CacheException
    {
        KAuthFile kauth = loadKauthFile();

        for (PasswordCredential password: subject.getPrivateCredentials(PasswordCredential.class)) {
            UserPwdRecord record =
                kauth.getUserPwdRecord(password.getUsername());
            if (record == null || record.isDisabled()) {
                throw new PermissionDeniedCacheException("Access denied");
            }

            if (!record.isAnonymous() &&
                !record.passwordIsValid(password.getPassword())) {
                throw new PermissionDeniedCacheException("Access denied");
            }

            if (record.isAnonymous()) {
                subject = Subjects.NOBODY;
            } else {
                subject = Subjects.getSubject(record, true);
            }
            return new LoginReply(subject, toLoginAttributes(record));
        }
        throw new IllegalArgumentException("Subject is not supported by KAuthFileLoginStrategy");
    }

    private Set<LoginAttribute> toLoginAttributes(UserAuthBase record)
    {
        Set<LoginAttribute> attributes = new HashSet<>();
        attributes.add(new HomeDirectory(record.Home));
        attributes.add(new RootDirectory(record.Root));
        if (record.ReadOnly) {
            attributes.add(Restrictions.readOnly());
        }
        return attributes;
    }

    @Override
    public Principal map(Principal principal) throws CacheException
    {
        return null;
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws CacheException
    {
        return Collections.emptySet();
    }
}
