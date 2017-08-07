package org.dcache.services.ssh2;

import org.apache.sshd.common.Factory;
import org.apache.sshd.common.FactoryManager;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.PropertyResolverUtils;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.common.keyprovider.AbstractFileKeyPairProvider;
import org.apache.sshd.common.session.Session;
import org.apache.sshd.common.session.SessionListener;
import org.apache.sshd.common.util.SecurityUtils;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator;
import org.apache.sshd.server.session.ServerSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import diskCacheV111.util.AuthorizedKeyParser;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.util.Files;
import org.dcache.util.NetLoggerBuilder;

import static java.util.stream.Collectors.toList;

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
    private static final Logger _log = LoggerFactory.getLogger(Ssh2Admin.class);
    private static final Logger _accessLog =
            LoggerFactory.getLogger("org.dcache.access.ssh2");
    private final SshServer _server;
    // UniversalSpringCell injected parameters
    private List<File> _hostKeys;
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

    public void setHostKeys(String[] keys) {
        _hostKeys = Stream.of(keys).map(File::new).collect(toList());
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
        } catch (IOException e) {
            _log.warn("SSH failure during shutdown: " + e.getMessage());
        }
    }

    private void configureKeyFiles() {
        try {
            for (File key : _hostKeys) {
                Files.checkFile(key);
            }
            AbstractFileKeyPairProvider fKeyPairProvider = SecurityUtils.createFileKeyPairProvider();
            fKeyPairProvider.setFiles(_hostKeys);
            _server.setKeyPairProvider(fKeyPairProvider);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void startServer() {
        long effectiveTimeout = _idleTimeoutUnit.toMillis(_idleTimeout);
        PropertyResolverUtils.updateProperty(_server, FactoryManager.IDLE_TIMEOUT, effectiveTimeout);
        // esure, that read timeout is longer than idle timeout
        PropertyResolverUtils.updateProperty(_server, FactoryManager.NIO2_READ_TIMEOUT, effectiveTimeout*2);

        _server.setPort(_port);
        _server.setHost(_host);
        _server.addSessionListener(new AdminConnectionLogger());

        try {
            _server.start();
        } catch (IOException ioe) {
            throw new RuntimeException("Ssh2 server was interrupted while starting: ", ioe);
        }
    }

    private void addOrigin(ServerSession session, Subject subject)
    {
        SocketAddress remote = session.getIoSession().getRemoteAddress();
        if (remote instanceof InetSocketAddress) {
            InetAddress address = ((InetSocketAddress) remote).getAddress();
            subject.getPrincipals().add(new Origin(address));
        }
    }

    private void logLoginTry(String username, ServerSession session,
                             String method, boolean successful, String reason)
    {
        NetLoggerBuilder.Level logLevel = NetLoggerBuilder.Level.INFO;
        if (!successful) {
            logLevel = NetLoggerBuilder.Level.WARN;
        }

        new NetLoggerBuilder(logLevel, "org.dcache.services.ssh2.login")
                .omitNullValues()
                .add("username", username)
                .add("remote.socket", session.getClientAddress())
                .add("method", method)
                .add("successful", successful)
                .add("reason", reason)
                .toLogger(_accessLog);

        // set session parameter
        if (successful) {
            try {
                session.setAuthenticated();
                session.setUsername(username);
            } catch (IOException e) {
                _log.error("Failed to set Authenticated: {}",
                        e.getMessage());
            }
        }
    }

    private class AdminPasswordAuthenticator implements PasswordAuthenticator {

        @Override
        public boolean authenticate(String userName, String password,
                ServerSession session) {
            boolean successful = false;
            String reason = null;
            Subject subject = new Subject();
            addOrigin(session, subject);
            subject.getPrivateCredentials().add(new PasswordCredential(userName,
                    password));

            try {
                LoginReply reply = _loginStrategy.login(subject);

                Subject authenticatedSubject = reply.getSubject();
                if (!Subjects.hasGid(authenticatedSubject, _adminGroupId)) {
                    reason = "not member of admin gid";
                    throw new PermissionDeniedCacheException(reason);
                }

                successful = true;
            } catch (PermissionDeniedCacheException e) {
                _log.warn("Login for {} denied: {}", userName, e.getMessage());
            } catch (CacheException e) {
                reason = e.toString();
                _log.warn("Login for {} failed: {}", userName, e.toString());
            }

            logLoginTry(userName, session, "Password", successful, reason);
            return successful;
        }
    }

    private class AdminPublickeyAuthenticator implements PublickeyAuthenticator {

        private PublicKey toPublicKey(String s) {
            try {
                AuthorizedKeyParser decoder = new AuthorizedKeyParser();
                return decoder.decodePublicKey(s);
            } catch (InvalidKeySpecException | NoSuchAlgorithmException | IllegalArgumentException e) {
                _log.warn("can't decode public key from file: {} ", e.getMessage());
            }
            return null;
        }

        @Override
        public boolean authenticate(String userName, PublicKey key,
                ServerSession session) {
            boolean successful = false;
            _log.debug("Authentication username set to: {} publicKey: {}",
                    userName, key);
            try {
                try(Stream<String> fileStream = java.nio.file.Files.lines(_authorizedKeyList.toPath())) {
                    successful =
                        fileStream
                        .filter(l -> !l.isEmpty() && !l.matches(" *#.*"))
                        .map(this::toPublicKey)
                        .filter(k -> k != null)
                        .filter(key::equals)
                        .findFirst()
                        .isPresent();
                }
            } catch (FileNotFoundException e) {
                _log.debug("File not found: {}", _authorizedKeyList);
            } catch (IOException e) {
                _log.error("Failed to read {}: {}", _authorizedKeyList,
                        e.getMessage());
            }

            // the method gets called twice while pubkey authentication,
            // to avoid duplicate logging, check if already authenticated
            if (!session.isAuthenticated()) {
                String method = "PublicKey (" + key.getAlgorithm() + " "
                        + KeyUtils.getFingerPrint(key) + ")";
                logLoginTry(userName, session, method, successful, null);
            }
            return successful;
        }
    }

    private class AdminConnectionLogger implements SessionListener {

        @Override
        public void sessionCreated(Session session) {
            logEvent("org.dcache.services.ssh2.connect", session);
        }

        @Override
        public void sessionClosed(Session session) {
            logEvent("org.dcache.services.ssh2.disconnect", session);
        }

        private void logEvent(String name, Session session) {
            new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, name)
                    .omitNullValues()
                    .add("username", session.getUsername())
                    .add("remote.socket", session.getIoSession()
                            .getRemoteAddress())
                    .toLogger(_accessLog);
        }
    }

}
