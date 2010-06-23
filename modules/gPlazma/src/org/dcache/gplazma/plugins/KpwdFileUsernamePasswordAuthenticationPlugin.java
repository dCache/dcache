package org.dcache.gplazma.plugins;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.Set;
import org.dcache.auth.KAuthFile;
import org.dcache.auth.UserPwdRecord;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.HomeDirectory;
import org.dcache.gplazma.RootDirectory;
import org.dcache.gplazma.SessionAttribute;
import org.dcache.gplazma.SessionID;

/**
 * Implementation of UsernamePasswordAuthenticationPlugin using a Kpwdfile as
 * authentication mechanism
 * @author jans
 */
public class KpwdFileUsernamePasswordAuthenticationPlugin
        extends UsernamePasswordAuthenticationPlugin {

    private final File _file;

    public KpwdFileUsernamePasswordAuthenticationPlugin(String[] arguments) {
        super(arguments);
        String kpwdFilePath = parseFilepath(arguments);
        File file = new File(kpwdFilePath);
        if (!file.canRead()) {
            throw new IllegalArgumentException("File not found: " + file);
        }
        _file = file;
    }

    private KAuthFile loadKauthFile() throws AuthenticationException {
        try {
            return new KAuthFile(_file.getPath());
        } catch (IOException e) {
            throw new AuthenticationException("Password file not found");
        }
    }

    @Override
    public void authenticate(String username, char[] password)
            throws AuthenticationException {
        KAuthFile kauthFile = loadKauthFile();
        UserPwdRecord userEntry = kauthFile.getUserPwdRecord(username);
        if ((userEntry == null) || !(userEntry.passwordIsValid(password.toString()))) {
            throw new AuthenticationException("Couldn't authenticate user");
        }
    }

    @Override
    protected void session(String username, Set<SessionAttribute> attrib) throws AuthenticationException {
        KAuthFile kauthFile = loadKauthFile();
        UserPwdRecord userEntry = kauthFile.getUserPwdRecord(username);
        if (userEntry == null) {
            throw new AuthenticationException("Userentry not present in Kauthfile");
        }
        attrib.add(new HomeDirectory(userEntry.Home));
        attrib.add(new RootDirectory(userEntry.Root));
    }

    private String parseFilepath(String[] arguments) {
        String kpwdFilePath = arguments[0];
        if ((kpwdFilePath == null) ||
                (kpwdFilePath.length() == 0) ||
                (!new File(kpwdFilePath).exists())) {
            throw new IllegalArgumentException(
                    "kpwdfile argument wasn't specified correctly");
        }
        return kpwdFilePath;
    }

    @Override
    public void reverseMap(SessionID sID, Principal sourcePrincipal, Set<Principal> principals) throws AuthenticationException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
