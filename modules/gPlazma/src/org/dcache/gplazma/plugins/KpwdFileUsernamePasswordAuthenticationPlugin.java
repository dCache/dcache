package org.dcache.gplazma.plugins;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.KAuthFile;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserPwdRecord;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;

/**
 * Implementation of UsernamePasswordAuthenticationPlugin using a Kpwdfile as
 * authentication mechanism
 * @author jans
 */
public class KpwdFileUsernamePasswordAuthenticationPlugin
        extends UsernamePasswordAuthenticationPlugin {

    private static final String KPWD_FILE = "kpwdfile";

    private final File _file;

    public KpwdFileUsernamePasswordAuthenticationPlugin(Properties properties) {
        super(properties);

        String kpwdFilePath = checkNotNull(properties.getProperty(KPWD_FILE), "Error: No KAuthFile specified.");

        File file = new File(kpwdFilePath);

        if (!file.canRead()) {
            throw new IllegalArgumentException(String.format("Error reading KAuthFile '%s'.", file));
        }

        _file = file;
    }

    private KAuthFile loadKauthFile() throws AuthenticationException {
        try {
            return new KAuthFile(_file.getPath());
        } catch (IOException e) {
            throw new AuthenticationException(String.format("Error loading KAuthFile '%s'.", _file));
        }
    }

    @Override
    public void authenticate(String username, char[] password)
            throws AuthenticationException {
        KAuthFile kauthFile = loadKauthFile();
        UserPwdRecord userEntry = kauthFile.getUserPwdRecord(username);
        if ((userEntry == null) || !(userEntry.passwordIsValid(String.valueOf(password)))) {
            throw new AuthenticationException("Unknown username/password combination.");
        }
    }

    @Override
    protected void map(String username, Set<Principal> principals,
            Set<Principal> authorizedPrincipals) throws AuthenticationException {
        KAuthFile kauthFile = loadKauthFile();
        UserPwdRecord userEntry = kauthFile.getUserPwdRecord(username);
        UidPrincipal uid = new UidPrincipal(userEntry.UID);
        authorizedPrincipals.add(uid);
        GidPrincipal gid = new GidPrincipal(userEntry.GID, true);
        authorizedPrincipals.add(gid);
    }

    @Override
    protected void session(String username, Set<Object> attrib)
        throws AuthenticationException
    {
        KAuthFile kauthFile = loadKauthFile();
        UserPwdRecord userEntry = kauthFile.getUserPwdRecord(username);
        if (userEntry == null) {
            throw new AuthenticationException("Userentry not present in Kauthfile");
        }
        attrib.add(new HomeDirectory(userEntry.Home));
        attrib.add(new RootDirectory(userEntry.Root));
        attrib.add(new ReadOnly(userEntry.ReadOnly));
    }

    @Override
    public void reverseMap(SessionID sID, Principal sourcePrincipal, Set<Principal> principals) throws AuthenticationException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
