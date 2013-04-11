package org.dcache.services.httpd;

import com.google.common.collect.Maps;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Args;

import org.dcache.cells.AbstractCell;
import org.dcache.cells.Option;
import org.dcache.services.httpd.handlers.HandlerDelegator;
import org.dcache.services.httpd.util.AliasEntry;

public class HttpServiceCell extends AbstractCell implements EnvironmentAware {
    private static final String IPV4_INETADDR_ANY = "0.0.0.0";
    private static final Logger logger
        = LoggerFactory.getLogger(HttpServiceCell.class);
    private final ConcurrentMap<String, AliasEntry> aliases
        = Maps.newConcurrentMap();

    @Option(name = "webappsResource",
            description = "Path to the resource URL for webapps default")
    protected String webappResourceUrl;

    @Option(name = "authenticated",
            description = "Enablement of secure connection (HTTPS)",
            defaultValue = "false")
    protected Boolean authenticated;

    @Option(name = "httpPort",
            description = "Main port for the service")
    protected Integer httpPort;

    @Option(name = "maxIdleTime",
            description = "Maximum idle time on connection",
            unit = "ms")
    protected Integer maxIdleTime;

    @Option(name = "maxThreads",
            description = "Maximum number of active threads")
    protected Integer maxThreads;

    /*
     * Authenticated settings
     */
    @Option(name = "httpsPort",
            description = "Port for secure access (SSL)")
    protected Integer httpsPort;

    @Option(name = "keystore",
            description = "Path to the file containing "
                        + "the encoded server certificate")
    protected String keystore;

    @Option(name = "keystoreType",
            description = "Encoding method",
            defaultValue = "PKCS12")
    protected String keystoreType;

    @Option(name = "keystorePassword",
            description = "Password for accessing server certificate")
    protected String keystorePassword;

    @Option(name = "truststore",
            description = "Path to Java Keystore containing the "
                                    + "trusted CA certicates used for SSL")
    protected String truststore;

    @Option(name = "truststorePassword",
            description = "Password for accessing trusted CA certs")
    protected String trustPassword;

    private Server server;
    private String defaultWebappsXml;
    private volatile Map<String, Object> environment = Collections.emptyMap();

    public HttpServiceCell(String name, String args)
                    throws InterruptedException, ExecutionException {
        super(name, args);
        doInit();
    }

    public static final String hh_ls_alias = "[<alias>]";

    public String ac_ls_alias_$_0_1(Args args) throws Exception {
        if (args.argc() == 0) {
            final StringBuilder sb = new StringBuilder();
            for (final Map.Entry<String, AliasEntry> aliasEntry : aliases.entrySet()) {
                sb.append(aliasEntry.getKey()).append(" -> ").append(
                                aliasEntry.getValue()).append("\n");
            }
            return sb.toString();
        } else {
            final AliasEntry entry = aliases.get(args.argv(0));
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
        final AliasEntry entry = AliasEntry.createEntry(args, this);
        logger.debug("putting {}, {}", entry.getName(), entry);
        aliases.put(entry.getName(), entry);
        return entry.getStatusMessage();
    }

    public static final String hh_unset_alias = "<aliasName>";

    public String ac_unset_alias_$_1(Args args) {
        aliases.remove(args.argv(0));
        return "Done";
    }

    @Override
    public void cleanUp() {
        shutDownAliases();
        try {
            if (!server.isStopped()) {
                server.stop();
            }
            server.destroy();
        } catch (final Exception e) {
            logger.error("Failed to stop Jetty: {}", e.getMessage());
        }
        super.cleanUp();
    }

    public String getDefaultWebappsXml() {
        return defaultWebappsXml;
    }

    public Map<String, Object> getEnvironment() {
        return environment;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public Integer getHttpsPort() {
        return httpsPort;
    }

    @Override
    public void getInfo(PrintWriter pw) {
        for (final Map.Entry<String, AliasEntry> aliasEntry : aliases.entrySet()) {
            pw.println("<<<<< " + aliasEntry.getKey() + " >>>>>>>>>");
            aliasEntry.getValue().getInfo(pw);
        }
    }

    public Server getServer() {
        return server;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    @Override
    public void setEnvironment(Map<String, Object> environment) {
        if (environment != null) {
            this.environment = environment;
        }
    }

    @Override
    protected void init() throws Exception {
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
        final HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { new HandlerDelegator(aliases),
                        new DefaultHandler(), new RequestLogHandler() });
        server.setHandler(handlers);
    }

    private void createAndSetThreadPool() {
        final QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(maxThreads);
        server.setThreadPool(threadPool);
    }

    private Connector createSimpleConnector() {
        final Connector connector = new SelectChannelConnector();
        connector.setPort(httpPort);
        connector.setMaxIdleTime(maxIdleTime);
        return connector;
    }

    private Connector createSslConnector() {
        final SslSelectChannelConnector connector = new SslSelectChannelConnector();
        connector.setPort(httpsPort);
        connector.setHost(IPV4_INETADDR_ANY);
        final SslContextFactory factory = connector.getSslContextFactory();
        factory.setKeyStorePath(keystore);
        factory.setKeyStoreType(keystoreType);
        factory.setKeyStorePassword(keystorePassword);
        factory.setTrustStore(truststore);
        factory.setTrustStorePassword(trustPassword);
        factory.setWantClientAuth(true);
        factory.setNeedClientAuth(false);
        return connector;
    }

    private void shutDownAliases() {
        for (final AliasEntry entry : aliases.values()) {
            entry.shutdown();
        }
    }
}
