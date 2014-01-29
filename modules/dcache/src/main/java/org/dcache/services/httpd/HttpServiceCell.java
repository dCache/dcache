package org.dcache.services.httpd;

import com.google.common.collect.Maps;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.PrintWriter;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.DomainContextAware;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.CommandInterpreter;

import org.dcache.services.httpd.handlers.HandlerDelegator;
import org.dcache.services.httpd.util.AliasEntry;
import org.dcache.util.Args;
import org.dcache.util.Crypto;

public class HttpServiceCell extends CommandInterpreter
                             implements CellMessageReceiver,
                                        CellMessageSender,
                                        CellCommandListener,
                                        CellInfoProvider,
                                        DomainContextAware,
                                        EnvironmentAware {
    private static final Logger logger
        = LoggerFactory.getLogger(HttpServiceCell.class);
    private final ConcurrentMap<String, AliasEntry> aliases
        = Maps.newConcurrentMap();

    /**
     * Path to the resource URL for webapps default
     */
    private String webappResourceUrl;

    /**
     * Where the war should be unpacked
     */
    private String tmpUnpackDir;

    /**
     * Enablement of secure connection (HTTPS)
     */
    private boolean authenticated;

    /**
     * Host to set connector to
     */
    private String host;

    /**
     * Main port for the service
     */
    private int httpPort;

    /**
     * Maximum idle time on connection
     */
    private int maxIdleTime;

    /**
     * Maximum idle time unit
     */
    private TimeUnit maxIdleTimeUnit = TimeUnit.MILLISECONDS;

    /**
     * Maximum number of active threads
     */
    private int maxThreads;

    /**
     * Port for secure access (SSL)
     */
    private int httpsPort;

    /**
     * Path to the file containing the encoded server certificate
     */
    private String keystore;

    /**
     * Password for accessing server certificate
     */
    private String keystorePassword;

    /**
     * Path to Java Keystore containing the trusted CA certicates used for SSL
     */
    private String truststore;

    /**
     * Password for accessing trusted CA certs
     */
    private String trustPassword;

    private String cipherFlags;

    private CellEndpoint endpoint;
    private Server server;
    private String defaultWebappsXml;
    private Map<String, Object> domainContext;
    private Map<String, Object> environment;

    private static class HttpdRequestLog extends AbstractLifeCycle
        implements RequestLog {
        public void log(Request request, Response response) {
            logger.trace("request: {}; response: {}", request, response);
        }
    }

    public static final String hh_ls_alias = "[<alias>]";

    public String ac_ls_alias_$_0_1(Args args) throws Exception {
        if (args.argc() == 0) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, AliasEntry> aliasEntry : aliases.entrySet()) {
                sb.append(aliasEntry.getKey()).append(" -> ").append(
                                aliasEntry.getValue()).append("\n");
            }
            return sb.toString();
        } else {
            AliasEntry entry = aliases.get(args.argv(0));
            if (entry == null) {
                throw new Exception("Alias not found : " + args.argv(0));
            }
            return args.argv(0) + " -> " + entry;
        }
    }

    public static final String hh_set_alias = "<aliasName> directory|class|context <specification>";

    public static final String fh_set_alias = "set alias <alias>  <type> [<typeSpecific> <...>]\n"
                    + "   <type>             <specific> \n"
                    + "   directory          <fullDirectoryPath>\n"
                    + "   file               <fullFilePath> <arguments> <...>\n"
                    + "   class              <fullClassName> <...>\n"
                    + "   context            [options] <context> or  <contextNameStart>*\n"
                    + "                       options : -overwrite=<alias> -onError=<alias>\n"
                    + "   webapp             <webappsContext> <webappsPath> <tempUnpackDir> <...> \n"
                    + "   redirect           <forward-to-context>\n"
                    + "   predefined alias : <home>    =  default for http://host:port/ \n"
                    + "                      <default> =  default for any type or error \n";

    public String ac_set_alias_$_3_16(Args args) throws Exception {
        logger.debug("ac_set_alias_$_3_16 {}", args.toString());
        AliasEntry entry = AliasEntry.createEntry(args, this);
        logger.debug("putting {}, {}", entry.getName(), entry);
        aliases.put(entry.getName(), entry);
        return entry.getStatusMessage();
    }

    public static final String hh_unset_alias = "<aliasName>";

    public String ac_unset_alias_$_1(Args args) {
        aliases.remove(args.argv(0));
        return "Done";
    }

    public void cleanUp() {
        shutDownAliases();
        try {
            if (!server.isStopped()) {
                server.stop();
            }
            server.destroy();
        } catch (Exception e) {
            logger.error("Failed to stop Jetty: {}", e.getMessage());
        }
    }

    public String getDefaultWebappsXml() {
        return defaultWebappsXml;
    }

    public Map<String, Object> getDomainContext() {
        return domainContext;
    }

    public CellEndpoint getEndpoint() {
        return endpoint;
    }

    public Map<String, Object> getEnvironment() {
        return environment;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getHttpsPort() {
        return httpsPort;
    }

    @Override
    public CellInfo getCellInfo(CellInfo info) {
        return info;
    }

    @Override
    public void getInfo(PrintWriter pw) {
        for (Map.Entry<String, AliasEntry> aliasEntry : aliases.entrySet()) {
            pw.println("<<<<< " + aliasEntry.getKey() + " >>>>>>>>>");
            aliasEntry.getValue().getInfo(pw);
        }
    }

    public Server getServer() {
        return server;
    }

    public String getTmpUnpackDir() {
        return tmpUnpackDir;
    }

    public boolean getAuthenticated() {
        return authenticated;
    }

    @Required
    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Required
    public void setCipherFlags(String cipherFlags) {
        this.cipherFlags = cipherFlags;
    }

    @Override
    public void setDomainContext(Map<String, Object> context) {
        this.domainContext = context;
    }

    @Override
    public void setEnvironment(Map<String, Object> environment) {
        this.environment = environment;
    }

    @Required
    public void setHost(String host) {
        this.host = host;
    }

    @Required
    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    @Required
    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    @Required
    public void setMaxIdleTimeUnit(TimeUnit maxIdleTimeUnit) {
        this.maxIdleTimeUnit = maxIdleTimeUnit;
    }

    @Required
    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    @Required
    public void setHttpsPort(int httpsPort) {
        this.httpsPort = httpsPort;
    }

    @Required
    public void setKeystore(String keystore) {
        this.keystore = keystore;
    }

    @Required
    public void setKeystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    @Required
    public void setTmpUnpackDir(String tmpUnpackDir) {
        this.tmpUnpackDir = tmpUnpackDir;
    }

    @Required
    public void setTruststore(String truststore) {
        this.truststore = truststore;
    }

    @Required
    public void setTrustPassword(String trustPassword) {
        this.trustPassword = trustPassword;
    }

    @Required
    public void setWebappResourceUrl(String webappResourceUrl) {
        this.webappResourceUrl = webappResourceUrl;
    }

    public void initialize() throws Exception {
        URL url = HttpServiceCell.class.getResource(webappResourceUrl);
        defaultWebappsXml = url.toExternalForm();
        server = new Server(httpPort);
        createAndSetThreadPool();

        if (authenticated) {
            server.setConnectors(new Connector[] { createSimpleConnector(),
                                                   createSslConnector() });
        } else {
            server.setConnectors(new Connector[] { createSimpleConnector() });
        }

        createAndSetHandlers();

        try {
            logger.debug("starting server");
            server.start();
        } finally {
            if (server.isFailed()) {
                logger.error("server failure, calling cleanUp ...");
                cleanUp();
            }
        }
    }

    private void createAndSetHandlers() {
        HandlerCollection handlers = new HandlerCollection();
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        requestLogHandler.setRequestLog(new HttpdRequestLog());
        handlers.setHandlers(new Handler[] { new HandlerDelegator(aliases),
                        new DefaultHandler(), requestLogHandler });
        server.setHandler(handlers);
    }

    private void createAndSetThreadPool() {
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(maxThreads);
        server.setThreadPool(threadPool);
    }

    private Connector createSimpleConnector() {
        Connector connector = new SelectChannelConnector();
        connector.setHost(host);
        connector.setPort(httpPort);
        connector.setMaxIdleTime((int)maxIdleTimeUnit.toMillis(maxIdleTime));
        return connector;
    }

    @SuppressWarnings("deprecation")
    private Connector createSslConnector() {
        SslSelectChannelConnector connector = new SslSelectChannelConnector();
        connector.setHost(host);
        connector.setPort(httpsPort);
        connector.setExcludeCipherSuites(Crypto.getBannedCipherSuitesFromConfigurationValue(cipherFlags));
        SslContextFactory factory = connector.getSslContextFactory();
        factory.setKeyStorePath(keystore);
        factory.setKeyStoreType("PKCS12");
        factory.setKeyStorePassword(keystorePassword);
        factory.setTrustStore(truststore);
        factory.setTrustStorePassword(trustPassword);
        factory.setWantClientAuth(true);
        factory.setNeedClientAuth(false);
        return connector;
    }

    private void shutDownAliases() {
        for (AliasEntry entry : aliases.values()) {
            entry.shutdown();
        }
    }
}
