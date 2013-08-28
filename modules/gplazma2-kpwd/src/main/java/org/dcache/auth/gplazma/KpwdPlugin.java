package org.dcache.auth.gplazma;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.KAuthFile;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserAuthBase;
import org.dcache.auth.UserAuthRecord;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.UserPwdRecord;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A principal that represent an entry in a kpwd file.
 *
 * Used internally by the KpwdPlugin to pass along identifying
 * information between the auth, map, account, and session steps.
 */
class KpwdPrincipal
    implements Principal, Serializable
{
    private static final long serialVersionUID = -5104794169722666904L;
    private String _name;
    private long _uid;
    private long _gid;
    private boolean _isDisabled;
    private String _home;
    private String _root;
    private boolean _isReadOnly;

    public KpwdPrincipal(UserAuthBase record)
    {
        checkNotNull(record);
        _name = record.Username;
        _uid = record.UID;
        _gid = record.GID;
        _isDisabled =
            (record instanceof UserPwdRecord &&
             ((UserPwdRecord) record).isDisabled());
        _home = record.Home;
        _root = record.Root;
        _isReadOnly = record.ReadOnly;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    public long getUid()
    {
        return _uid;
    }

    public long getGid()
    {
        return _gid;
    }

    public boolean isDisabled()
    {
        return _isDisabled;
    }

    public String getHome()
    {
        return _home;
    }

    public String getRoot()
    {
        return _root;
    }

    public boolean isReadOnly()
    {
        return _isReadOnly;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + getName() + "]";
    }
}

public class KpwdPlugin
    implements GPlazmaAuthenticationPlugin,
               GPlazmaMappingPlugin,
               GPlazmaAccountPlugin,
               GPlazmaSessionPlugin
{
    private static Logger logger = LoggerFactory.getLogger(KpwdPlugin.class);
    private final static String KPWD = "gplazma.kpwd.file";

    private final File _kpwdFile;

    private long _cacheTime;
    private KAuthFile _cacheAuthFile;

    public KpwdPlugin(Properties properties)
    {
        String path = properties.getProperty(KPWD, null);
        if (path == null) {
            throw new IllegalArgumentException(KPWD + " argument must be specified");
        }
        _kpwdFile = new File(path);
    }

    /** Constructor for testing. */
    KpwdPlugin(KAuthFile file)
    {
        _cacheAuthFile = file;
        _kpwdFile = null;
    }

    private synchronized KAuthFile getAuthFile()
        throws AuthenticationException
    {
        try {
            if (_kpwdFile != null && _kpwdFile.lastModified() >= _cacheTime) {
                _cacheAuthFile = new KAuthFile(_kpwdFile.getPath());
                _cacheTime = System.currentTimeMillis();
            }
            return _cacheAuthFile;
        } catch (IOException e) {
            String msg = String.format("failed to read %s: %s",
                    _kpwdFile.getName(), e.getMessage());
            throw new AuthenticationException(msg, e);
        }
    }

    /**
     * Password authentication.
     *
     * Authenticates login name + password and generates a
     * KpwdPrincipal.
     */
    @Override
    public void authenticate(Set<Object> publicCredentials,
                             Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals)
        throws AuthenticationException
    {
        PasswordCredential password =
            getFirst(filter(privateCredentials, PasswordCredential.class), null);
        if (password == null) {
            throw new AuthenticationException("no username and password");
        }

        String name = password.getUsername();
        UserPwdRecord entry = getAuthFile().getUserPwdRecord(name);
        if (entry == null) {
            throw new AuthenticationException(name + " is unknown");
        }

        if (!entry.isAnonymous() &&
            !entry.isDisabled() &&
            !entry.passwordIsValid(String.valueOf(password.getPassword()))) {
            throw new AuthenticationException("wrong password");
        }

        /* WARNING: We add the principal even when the account is
         * banned and we do so without checking the password; this is
         * to allow blacklisting during the account step.
         */
        identifiedPrincipals.add(new KpwdPrincipal(entry));

        if (entry.isDisabled()) {
            throw new AuthenticationException("account is disabled");
        }
    }

    /**
     * Maps KpwdPrincipal, DN, and Kerberos Principal to UserName, UID
     * and GID.
     *
     * Authorizes user name, DN, Kerberos principal, UID and GID.
     */
    @Override
    public void map(Set<Principal> principals,
                    Set<Principal> authorizedPrincipals)
        throws AuthenticationException
    {
        KpwdPrincipal kpwd =
            getFirst(filter(principals, KpwdPrincipal.class), null);

        if (kpwd == null) {
            KAuthFile authFile = getAuthFile();

            String loginName = null;
            Principal principal = null;

            for (Principal p: principals) {
                if (p instanceof LoginNamePrincipal) {
                    if (loginName != null) {
                        throw new AuthenticationException(errorMessage(principal, p));
                    }
                    loginName = p.getName();
                } else if (p instanceof GlobusPrincipal) {
                    if (principal != null) {
                        throw new AuthenticationException(errorMessage(principal, p));
                    }
                    principal = p;
                } else if (p instanceof KerberosPrincipal) {
                    if (principal != null) {
                        throw new AuthenticationException(errorMessage(principal, p));
                    }
                    principal = p;
                } else if (p instanceof UserNamePrincipal) {
                    /*
                     * This case handles e.g. authenticated dcap
                     * doors, particularly statement like
                     *
                     * mapping "user1" user2
                     *
                     * in kpwd file
                     */
                    checkAuthentication(principal == null,
                            errorMessage(principal, p));
                    principal = p;
                }
            }

            if (principal == null) {
                throw new AuthenticationException("no mappable principals");
            }

            if (loginName == null) {
                loginName = authFile.getIdMapping(principal.getName());
                if (loginName == null) {
                    throw new AuthenticationException("no login name");
                }
            }

            UserAuthRecord authRecord = authFile.getUserRecord(loginName);

            if (authRecord == null) {
                throw new AuthenticationException("unknown login name: " + loginName);
            }

            if (!authRecord.hasSecureIdentity(principal.getName())) {
                throw new AuthenticationException("not allowed to login as " +
                        loginName);
            }

            authRecord.DN = principal.getName();
            kpwd = new KpwdPrincipal(authRecord);

            if (!(principal instanceof UserNamePrincipal)) {
                /*
                 * besides GlobusPrincipal, KerberosPrincipal, LoginNamePrincipal
                 * kpwd file can contain UserNamePrincipal that can be mapped to
                 * another UserNamePrincipal. To avoid issue with multiple usernames
                 * we skip adding principal if it is UserNamePrincipal
                 *
                 */
                authorizedPrincipals.add(principal);
            }
        }

        authorizedPrincipals.add(kpwd);

        /* We explicitly check whether the user record is banned and
         * don't authorize the remaining principals. We do however
         * authorize the KpwdPrincipal to allow blacklisting in the
         * account step.
         */
        if (kpwd.isDisabled()) {
            throw new AuthenticationException("account disabled");
        }

        authorizedPrincipals.add(new UserNamePrincipal(kpwd.getName()));
        authorizedPrincipals.add(new UidPrincipal(kpwd.getUid()));
        authorizedPrincipals.add(new GidPrincipal(kpwd.getGid(), true));
    }

    private static String errorMessage(Principal principal1,
            Principal principal2)
    {
        String name1 = nameFor(principal1);
        String name2 = nameFor(principal2);

        if (name1.equals(name2)) {
            return "multiple " + name2 + " principals found";
        } else {
            return name1 + " and " + name2 + " principals found";
        }
    }

    private static String nameFor(Principal principal)
    {
        if(principal instanceof KerberosPrincipal) {
            return "Kerberos";
        } else if(principal instanceof GlobusPrincipal) {
            return "X509";
        } else {
            return principal.getClass().getSimpleName();
        }
    }


    /**
     * Checks whether KpwdPrincipal is flagged as disabled.
     */
    @Override
    public void account(Set<Principal> authorizedPrincipals)
                throws AuthenticationException
    {
        KpwdPrincipal kpwd =
            getFirst(filter(authorizedPrincipals, KpwdPrincipal.class), null);
        if (kpwd != null && kpwd.isDisabled()) {
            throw new AuthenticationException("account disabled");
        }
    }

    /**
     * Assigns home, root and read only attributes from KpwdPrincipal.
     */
    @Override
    public void session(Set<Principal> authorizedPrincipals,
                        Set<Object> attrib)
        throws AuthenticationException
    {
        KpwdPrincipal kpwd =
            getFirst(filter(authorizedPrincipals, KpwdPrincipal.class), null);
        if (kpwd == null) {
            throw new AuthenticationException("no record found");
        }

        attrib.add(new HomeDirectory(kpwd.getHome()));
        attrib.add(new RootDirectory(kpwd.getRoot()));
        attrib.add(new ReadOnly(kpwd.isReadOnly()));
    }
}
