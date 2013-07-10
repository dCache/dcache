package org.dcache.services.ssh2;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.sshd.SshServer;
import org.apache.sshd.common.keyprovider.FileKeyPairProvider;
import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.PublickeyAuthenticator;
import org.apache.sshd.server.session.ServerSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.List;

import diskCacheV111.util.AuthorizedKeyParser;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.auth.UnionLoginStrategy;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellLifeCycleAware;
import org.dcache.cells.CellMessageSender;

import static org.dcache.util.Files.checkFile;

/**
 * This class starts the ssh server. It is however not started in the
 * constructor, but in afterStart() to avoid race conditions. The class starts
 * the UserAdminShell via the factory CommandFactory, which in turn create the
 * Command_ConsoleReader that actually creates an instance of UserAdminShell.
 *
 * @author bernardt
 */
public class Ssh2Admin implements CellCommandListener, CellMessageSender,
        CellLifeCycleAware {

    private final static Logger _log = LoggerFactory.getLogger(Ssh2Admin.class);
    private final SshServer _server;
    // UniversalSpringCell injected parameters
    private String _hostKeyPrivate;
    private String _hostKeyPublic;
    private File _authorizedKeyList;
    private int _port;
    private int _adminGroupId;
    private File _historyFile;
    private LoginStrategy _loginStrategy;
    // Cell Functionality
    private CellEndpoint _cellEndPoint;

    public Ssh2Admin() {
        _server = SshServer.setUpDefaultServer();
    }

    public LoginStrategy getLoginStrategy() {
        return _loginStrategy;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy) {
        _loginStrategy = loginStrategy;
    }

    public File getHistoryFile() {
        return _historyFile;
    }

    public void setHistoryFile(File historyFile) {
        _historyFile = historyFile;
    }

    public void setPort(int port) {
        _log.debug("Ssh2 port set to: {}", String.valueOf(port));
        _port = port;
    }

    public int getPort() {
        return _port;
    }

    public void setAdminGroupId(int groupId) {
        _adminGroupId = groupId;
    }

    public int getAdminGroupId() {
        return _adminGroupId;
    }

    public String getHostKeyPrivate() {
        return _hostKeyPrivate;
    }

    public void setHostKeyPrivate(String hostKeyPrivate) {
        _hostKeyPrivate = hostKeyPrivate;
        _log.debug("hostKeyPrivate set to: {}", _hostKeyPrivate);
    }

    public String getHostKeyPublic() {
        return _hostKeyPublic;
    }

    public void setHostKeyPublic(String hostKeyPublic) {
        _hostKeyPublic = hostKeyPublic;
        _log.debug("hostKeyPublic set to: {}", _hostKeyPublic);
    }

    public File getAuthorizedKeyList() {
        return _authorizedKeyList;
    }

    public void setAuthorizedKeyList(File authorizedKeyList) {
        _authorizedKeyList = authorizedKeyList;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _cellEndPoint = endpoint;
        _log.debug("CellEndpoint set to: {}", _cellEndPoint);
    }

    public void setServerShellFactory(String userName) {
        CommandFactory factory = new CommandFactory(userName, _cellEndPoint,
                _historyFile);
        _server.setShellFactory(factory);
    }

    public void configureAuthentication() {
        _server.setPasswordAuthenticator(new AdminPasswordAuthenticator());
        _server.setPublickeyAuthenticator(new AdminPublickeyAuthenticator());
    }

    public boolean kpwdLogin(String userName, String passwd) {
        PasswordCredential passCredential = new PasswordCredential(userName,
                passwd);
        Subject subject = new Subject();
        subject.getPrivateCredentials().add(passCredential);

        try {
            _log.debug("LoginStrategy: {}, {}", _loginStrategy.getClass(),
                    ((UnionLoginStrategy) _loginStrategy).getLoginStrategies());
            LoginReply loginReply = _loginStrategy.login(subject);
            Subject authenticatedSubject = loginReply.getSubject();
            String authenticatedUsername =  Subjects.getDisplayName(authenticatedSubject);
            _log.debug("All pricipals returned by login: {}", authenticatedSubject.getPrincipals());
            if (Subjects.hasGid(authenticatedSubject, _adminGroupId)) {
                setServerShellFactory(authenticatedUsername);
                return true;
            } else {

                long[] userGids = Subjects.getGids(authenticatedSubject);
                _log.warn("User: " + authenticatedUsername
                        + " has GID(s): " + Arrays.toString(userGids) + "."
                        + " In order to have login rights this list should"
                        + " include GID " + _adminGroupId + ". Add GID "
                        + _adminGroupId + " to the user's GID list to grant"
                        + " login rights.");
                return false;
            }
        } catch (PermissionDeniedCacheException e) {
            _log.warn("Pwd-based login for user: {} was denied.", userName);
        } catch (CacheException e) {
            _log.warn("Pwd-based Login failed: {}", e.toString());
        }
        return false;
    }

    @Override
    public void afterStart() {
        configureAuthentication();
        configureKeyFiles();
        startServer();

        _log.debug("Ssh2 Admin Interface started!");
    }

    @Override
    public void beforeStop() {
        try {
            _server.stop();
        } catch (InterruptedException e) {
            _log.warn("Server was interupted during shutdown!");
        }
    }

    private void configureKeyFiles() {
        try {
            checkFile(_hostKeyPrivate);
            checkFile(_hostKeyPublic);
        } catch (IOException ex) {
            throw new RuntimeException("Problem with server ssh host keys, " + ex.getMessage());
        }

        String[] keyFiles = {_hostKeyPrivate, _hostKeyPublic};
        FileKeyPairProvider fKeyPairProvider = new FileKeyPairProvider(
                keyFiles);

        _server.setKeyPairProvider(fKeyPairProvider);

    }

    private void startServer() {
        _server.setPort(_port);

        try {
            _server.start();
        } catch (IOException ioe) {
            throw new RuntimeException("Ssh2 server was interrupted while starting: ", ioe);
        }
    }

    private class AdminPasswordAuthenticator implements PasswordAuthenticator {

        @Override
        public boolean authenticate(String userName, String password,
                ServerSession session) {
            _log.debug("Authentication username set to: {}", userName);
            return kpwdLogin(userName, password);
        }
    }

    private class AdminPublickeyAuthenticator implements PublickeyAuthenticator {

        @Override
        public boolean authenticate(String userName, PublicKey key,
                ServerSession session) {
            _log.debug("Authentication username set to: {} publicKey: {}",
                    userName, key);
            try {
                AuthorizedKeyParser decoder = new AuthorizedKeyParser();
                List<String> keyLines =
                        Files.readLines(_authorizedKeyList, Charsets.UTF_8);
                for (String keyLine : keyLines) {
                    PublicKey decodedKey = decoder.decodePublicKey(keyLine);
                    if (decodedKey.equals(key)) {
                        _log.debug("Key found! Decoded Key:"
                                + " {}, SshReceivedKey: {}", decodedKey, key);
                        setServerShellFactory(userName);
                        return true;
                    }
                }
            } catch (FileNotFoundException e) {
                _log.debug("File not found: {}", _authorizedKeyList);
                return false;
            } catch (IOException e) {
                _log.error("Failed to read {}: {}", _authorizedKeyList,
                        e.getMessage());
                return false;
            } catch (IllegalArgumentException e) {
                _log.warn("One of the keys in {} is of an unknown type: {}",
                        _authorizedKeyList, e.getMessage());
            } catch (InvalidKeySpecException e) {
                _log.warn("One of the keys in {} has an unknown key "
                        + "specification.", _authorizedKeyList);
            } catch (NoSuchAlgorithmException e) {
                _log.warn("The cryptographic algorithm of one of the "
                        + "keys in {} is not known.", _authorizedKeyList);
            }
            _log.debug("User {} failed authenticate since supplied {} not in {}",
                    userName, key.getAlgorithm(), _authorizedKeyList);
            return false;
        }
    }
}
