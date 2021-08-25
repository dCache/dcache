package org.dcache.services.ssh2;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.apache.sshd.common.AttributeRepository;
import org.apache.sshd.common.config.keys.AuthorizedKeyEntry;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.common.keyprovider.FileKeyPairProvider;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.common.session.Session;
import org.apache.sshd.common.session.SessionListener;
import org.apache.sshd.core.CoreModuleProperties;
import org.apache.sshd.server.auth.pubkey.AuthorizedKeyEntriesPublickeyAuthenticator;
import org.apache.sshd.server.command.CommandFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.gss.GSSAuthenticator;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.config.keys.AuthorizedKeysAuthenticator;
import org.apache.sshd.server.session.ServerSession;

import org.apache.sshd.server.shell.ShellFactory;
import org.apache.sshd.server.subsystem.SubsystemFactory;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.util.Glob;
import org.dcache.util.Subnet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;

import org.dcache.util.Files;
import org.dcache.util.NetLoggerBuilder;

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

    /**
     * Attribute to avoid double logging of public kay based logins.
     */
    public static final AttributeRepository.AttributeKey<Boolean> LOGGED = new AttributeRepository.AttributeKey<>();

    // get the user part from a public key line like
    // ... ssh-rsa AAAA...... user@hostname
    private static final Pattern _pubKeyFileUsername =
            Pattern.compile("(.*?)@.*");

    private final SshServer _server;
    // UniversalSpringCell injected parameters
    private Path _hostKey;
    private File _authorizedKeyList;
    private String _host;
    private int _port;
    private int _adminGroupId;
    private LoginStrategy _loginStrategy;
    private TimeUnit _idleTimeoutUnit;
    private long _idleTimeout;
    private enum Outcome { ALLOW, DENY, DEFER }
    // switches for different Authenticators
    private enum AuthenticationMechanism {KERBEROS, PASSWORD, PUBLICKEY}
    private EnumSet<AuthenticationMechanism>  enabledAuthenticationMechanisms;
    private String keyTabFile;

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

    public void setHostKeys(String key) throws IOException {
        Files.checkFile(key);
        _hostKey = Paths.get(key);
    }

    public File getAuthorizedKeyList() {
        return _authorizedKeyList;
    }

    public void setAuthorizedKeyList(File authorizedKeyList) {
        _authorizedKeyList = authorizedKeyList;
    }

    @Required
    public void setShellFactory(ShellFactory shellCommand) {
        _server.setShellFactory(shellCommand);
    }

    @Required
    public void setCommandFactory(CommandFactory commandFactory) {
        _server.setCommandFactory(commandFactory);
    }

    @Required
    public void setSubsystemFactories(List<SubsystemFactory> subsystemFactories)
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

    @Required
    public void setEnabledAuthenticationMechanisms(String mechanisms) {
        enabledAuthenticationMechanisms = EnumSet.copyOf(
                Arrays.stream(
                        Iterables.toArray(
                                Splitter.on(',')
                                        .trimResults()
                                        .split(mechanisms.toUpperCase()),
                                String.class))
                        .map(AuthenticationMechanism::valueOf)
                        .collect(Collectors.toList()));
    }

    @Required
    public void setKeyTabFile(String name) {
        keyTabFile = name;
    }

    private void configureAuthentication() {
        _server.setGSSAuthenticator(enabledAuthenticationMechanisms.contains(AuthenticationMechanism.KERBEROS) ?
                new AdminGssAuthenticator() : null);
        _server.setPasswordAuthenticator(enabledAuthenticationMechanisms.contains(AuthenticationMechanism.PASSWORD) ?
                new AdminPasswordAuthenticator() : null);
        _server.setPublickeyAuthenticator(enabledAuthenticationMechanisms.contains(AuthenticationMechanism.PUBLICKEY) ?
                new AdminPublickeyAuthenticator() : null);
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
            _log.warn("SSH failure during shutdown: {}", e.getMessage());
        }
    }

    private void configureKeyFiles() {
        KeyPairProvider keyPair = new FileKeyPairProvider(_hostKey);
        _server.setKeyPairProvider(keyPair);
    }

    private void startServer() {
        // Duration#of is picky about overflows
        Duration effectiveTimeout = Duration.ofMillis(_idleTimeoutUnit.toMillis(_idleTimeout));
        CoreModuleProperties.IDLE_TIMEOUT.set(_server, effectiveTimeout);
        // ensure, that read timeout is longer than idle timeout
        CoreModuleProperties.NIO2_READ_TIMEOUT.set(_server, effectiveTimeout);

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
                .add("session", Sessions.connectionId(session))
                .add("username", username)
                .add("method", method)
                .add("successful", successful)
                .add("reason", reason)
                .toLogger(_accessLog);
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

    private class AdminPublickeyAuthenticator extends AuthorizedKeysAuthenticator {

        public AdminPublickeyAuthenticator() {
            super(_authorizedKeyList.toPath());
        }

        private String getUserFromKeyLine(String line) {
            Matcher m = _pubKeyFileUsername.matcher(line);
            if (m.matches()) {
                return m.group(1);
            }
            return "";
        }

        @Override
        public boolean authenticate(String userName, PublicKey key,
                ServerSession session) {

            _log.debug("Authentication username set to: {} publicKey: {}", userName, key);

            String reason = null;
            boolean successful = super.authenticate(userName, key, session);
            if (successful) {
                AuthorizedKeyEntry ke = session.getAttribute(AuthorizedKeyEntriesPublickeyAuthenticator.AUTHORIZED_KEY);
                String hostspec = ke.getLoginOptions().getOrDefault("from", "");
                if (!isValidHost(hostspec, session)) {
                    reason = "key file: publickey used from unallowed host";
                    successful = false;
                }
                String keyUsername = getUserFromKeyLine(ke.getComment());
                if (keyUsername.isEmpty()) {
                    reason = "key file: no username@host for publickey set";
                    successful = false;
                }
                if (!keyUsername.equals(userName)) {
                    reason = "key file: different username for "
                            + "publickey expected";
                    successful = false;
                }
            } else {
                reason = "key file: no matching key found";
            }

            // the method gets called twice while pubkey authentication,
            // to avoid duplicate logging, check if already logged
            // REVISIT: we need to investigate the reason of duplicate logging
            if(session.getAttribute(LOGGED) == null) {
                String method = "PublicKey (" + key.getAlgorithm() + " " + KeyUtils.getFingerPrint(key) + ")";
                logLoginTry(userName, session, method, successful, reason);
                session.setAttribute(LOGGED, true);
            }
            return successful;
        }

        private boolean isValidHost(String hostSpec, ServerSession session) {

            if (hostSpec.isEmpty()) {
                return true;
            }

            Set<Outcome> outcomes = EnumSet.noneOf(Outcome.class);
            for (String pattern : Splitter.on(",").trimResults().omitEmptyStrings().split(hostSpec)) {
                outcomes.add(patternMatchesHost(pattern, session));
            }
            return outcomes.contains(Outcome.ALLOW) && !outcomes.contains(Outcome.DENY);
        }

        private Outcome patternMatchesHost(String pattern, ServerSession session) {
            boolean patternIsNegated = pattern.startsWith("!");
            Outcome matchOutcome = patternIsNegated ? Outcome.DENY : Outcome.ALLOW;
            if (patternIsNegated) {
                pattern = pattern.substring(1);
            }
            SocketAddress rawRemote = session.getClientAddress();
            if (rawRemote instanceof InetSocketAddress) {
                InetSocketAddress remote = (InetSocketAddress) rawRemote;
                InetAddress remoteAddress = remote.getAddress();
                if (Subnet.isValid(pattern)) {
                    if (Subnet.create(pattern).contains(remoteAddress)) {
                        return matchOutcome;
                    }
                } else {
                    Glob glob = new Glob(pattern);
                    if (glob.matches(remote.getHostName()) || glob.matches(remoteAddress.getHostAddress())) {
                        return matchOutcome;
                    }
                }
            }
            return Outcome.DEFER;
        }
    }



    private class AdminConnectionLogger implements SessionListener {

        @Override
        public void sessionCreated(Session session) {
            new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.services.ssh2.connect")
                    .omitNullValues()
                    .add("session", Sessions.connectionId(session))
                    .add("remote.socket", session.getIoSession()
                            .getRemoteAddress())
                    .toLogger(_accessLog);
        }

        @Override
        public void sessionClosed(Session session) {
            new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.services.ssh2.disconnect")
                    .omitNullValues()
                    .add("session", Sessions.connectionId(session))
                    .toLogger(_accessLog);
        }
    }

    private class AdminGssAuthenticator extends GSSAuthenticator {

        AdminGssAuthenticator() {
            setKeytabFile(keyTabFile);
        }

        @Override
        public boolean validateInitialUser(ServerSession session, String user) {
            session.setUsername(user);
            return true;
        }

        @Override
        public boolean validateIdentity(ServerSession session, String identity) {
            boolean successful = false;
            String reason = null;
            Subject subject = new Subject();
            subject.getPrincipals().add(new KerberosPrincipal(identity));
            String userName = session.getUsername();
            if (userName != null) {
                subject.getPrincipals().add(new LoginNamePrincipal(userName));
            }
            addOrigin(session, subject);
            try {
                LoginReply reply = _loginStrategy.login(subject);
                Subject authenticatedSubject = reply.getSubject();
                if (!Subjects.hasGid(authenticatedSubject, _adminGroupId)) {
                    throw new PermissionDeniedCacheException("not member of admin gid");
                }
                successful = true;
            } catch (PermissionDeniedCacheException e) {
                _log.error("Login for {} denied: {}",
                           Strings.nullToEmpty(userName),
                           e.getMessage());
                reason = e.getMessage();
            } catch (CacheException e) {
                reason = e.toString();
                _log.error("Login for {} failed: {}",
                           Strings.nullToEmpty(userName),
                           e.toString());
            }
            logLoginTry(userName, session, "GSS", successful, reason);
            return successful;
        }
    }
}