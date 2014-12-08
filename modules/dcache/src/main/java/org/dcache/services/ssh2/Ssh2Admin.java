package org.dcache.services.ssh2;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import org.apache.sshd.SshServer;
import org.apache.sshd.common.Factory;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.keyprovider.FileKeyPairProvider;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.PasswordAuthenticator;
import org.apache.sshd.server.PublickeyAuthenticator;
import org.apache.sshd.server.session.ServerSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.AuthorizedKeyParser;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import org.dcache.auth.*;

import static java.util.stream.Collectors.toList;
import static org.dcache.util.Files.checkFile;

/**
 * This class starts the ssh server. It is however not started in the
 * constructor, but in afterStart() to avoid race conditions. The class starts
 * the UserAdminShell via the factory CommandFactory, which in turn create the
 * Command_ConsoleReader that actually creates an instance of UserAdminShell.
 *
 * @author bernardt
 */
public class Ssh2Admin implements CellCommandListener, CellLifeCycleAware
{
    private final static Logger _log = LoggerFactory.getLogger(Ssh2Admin.class);
    private final SshServer _server;
    // UniversalSpringCell injected parameters
    private String _hostKeyPrivate;
    private String _hostKeyPublic;
    private File _authorizedKeyList;
    private String _host;
    private int _port;
    private int _adminGroupId;
    private LoginStrategy _loginStrategy;
    private TimeUnit _idleTimeoutUnit;
    private long _idleTimeout;

    public Ssh2Admin() {
        _server = SshServer.setUpDefaultServer();
    }

    public LoginStrategy getLoginStrategy() {
        return _loginStrategy;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy) {
        _loginStrategy = loginStrategy;
    }

    public void setPort(int port) {
        _log.debug("Ssh2 port set to: {}", String.valueOf(port));
        _port = port;
    }

    public int getPort() {
        return _port;
    }

    public void setHost(String host) {
        _host = host;
    }

    public String getHost() {
        return _host;
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

    @Required
    public void setShellFactory(Factory<Command> shellCommand)
    {
        _server.setShellFactory(shellCommand);
    }

    @Required
    public void setSubsystemFactories(List<NamedFactory<Command>> subsystemFactories)
    {
        _server.setSubsystemFactories(subsystemFactories);
    }

    @Required
    public void setIdleTimeout(long timeout)
    {
        _idleTimeout = timeout;
    }

    @Required
    public void setIdleTimeoutUnit(TimeUnit unit) {
        _idleTimeoutUnit = unit;
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
            _log.debug("All pricipals returned by login: {}", authenticatedSubject.getPrincipals());
            if (Subjects.hasGid(authenticatedSubject, _adminGroupId)) {
                return true;
            } else {
                long[] userGids = Subjects.getGids(authenticatedSubject);
                _log.warn("User: {} has GID(s): {}. In order to have login " +
                        "rights this list should include GID {}. Add GID {} " +
                        "to the user's GID list to grant login rights.",
                        Subjects.getDisplayName(authenticatedSubject),
                        Arrays.toString(userGids), _adminGroupId, _adminGroupId);
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
        // MINA SSH uses int to store timeout. Strip the long valued to max int if required.
        int effectiveTimeout = (int)Math.min((long)Integer.MAX_VALUE,
                _idleTimeoutUnit.toMillis(_idleTimeout > 0? _idleTimeout : Long.MAX_VALUE));

        _server.getProperties().put(SshServer.IDLE_TIMEOUT, Integer.toString(effectiveTimeout));
        _server.setPort(_port);
        _server.setHost(_host);

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

        private final Logger _logger = LoggerFactory.getLogger(AdminPublickeyAuthenticator.class);

        @Override
        public boolean authenticate(String userName, PublicKey key,
                ServerSession session) {
            _log.debug("Authentication username set to: {} publicKey: {}",
                    userName, key);
            try {
                AuthorizedKeyParser decoder = new AuthorizedKeyParser();
                List<String> keyLines = java.nio.file.Files.lines(_authorizedKeyList.toPath())
                        .filter(l -> !l.isEmpty() && !l.matches(" *#.*"))
                        .collect(toList());

                for (String keyLine : keyLines) {
                    PublicKey decodedKey = decoder.decodePublicKey(keyLine);
                    if (decodedKey.equals(key)) {
                        _log.debug("Key found! Decoded Key:"
                                + " {}, SshReceivedKey: {}", decodedKey, key);
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
