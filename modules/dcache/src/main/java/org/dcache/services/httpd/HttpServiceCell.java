package org.dcache.services.httpd;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.dcache.cells.AbstractCell;
import org.dcache.services.httpd.handlers.HandlerDelegator;
import org.dcache.services.httpd.util.AliasEntry;
import org.eclipse.jetty.http.ssl.SslContextFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Args;

public class HttpServiceCell extends AbstractCell implements EnvironmentAware {
    public static final String OPT_HTTP_PORT = "httpPort";
    public static final String OPT_HTTPS_PORT = "httpsPort";
    public static final String OPT_KEYSTORE = "keystore";
    public static final String OPT_KEYSTORE_TYPE = "keystoreType";
    public static final String OPT_KEYSTORE_PASSWORD = "keystorePassword";
    public static final String OPT_TRUSTSTORE = "truststore";
    public static final String OPT_TRUSTSTORE_PASSWORD = "truststorePassword";
    public static final String OPT_MAX_IDLE_TIME = "maxIdleTime";
    public static final String OPT_MAX_THREADS = "maxThreads";

    private static final String IPV4_INETADDR_ANY = "0.0.0.0";
    private static final int DEFAULT_PORT = 2288;
    private static final int MAX_IDLE_TIME = 30000;
    private static final int MAX_THREADS = 100;
    private static final Logger logger = LoggerFactory.getLogger(HttpServiceCell.class);

    private final Map<String, AliasEntry> aliases;

    private Integer httpPort = DEFAULT_PORT;
    private Integer httpsPort;
    private int maxIdleTime = MAX_IDLE_TIME;
    private int maxThreads = MAX_THREADS;

    private String keystore;
    private String keystoreType;
    private String keystorePassword;
    private String truststore;
    private String trustPassword;

    private Server server;
    private Map<String, Object> context;
    private volatile Map<String, Object> environment;

    public HttpServiceCell(String name, String args)
                    throws InterruptedException, ExecutionException {
        super(name, args);
        aliases = Maps.newHashMap();
        doInit();
    }

    public String hh_ls_alias = "[<alias>]";

    public String ac_ls_alias_$_0_1(Args args) throws Exception {
        AliasEntry entry = null;
        if (args.argc() == 0) {
            final StringBuilder sb = new StringBuilder();
            for (final Map.Entry<String, AliasEntry> aliasEntry : aliases.entrySet()) {
                sb.append(aliasEntry.getKey()).append(" -> ").append(
                                aliasEntry.getValue()).append("\n");
            }
            return sb.toString();
        } else {
            entry = aliases.get(args.argv(0));
            if (entry == null) {
                throw new Exception("Alias not found : " + args.argv(0));
            }
            return args.argv(0) + " -> " + entry;
        }
    }

    public String hh_set_alias = "<aliasName> directory|class|context <specification>";

    public String fh_set_alias = "set alias <alias>  <type> [<typeSpecific> <...>]\n"
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

    public String hh_unset_alias = "<aliasName>";

    public String ac_unset_alias_$_1(Args args) {
        aliases.remove(args.argv(0));
        return "Done";
    }

    @Override
    public void cleanUp() {
        try {
            server.stop();
        } catch (final Exception e) {
            logger.error("Failed to stop Jetty: {}", e.getMessage());
            server.destroy();
        } finally {
            shutdownEngines();
        }
        super.cleanUp();
    }

    public Map<String, Object> getEnvironment() {
        if (environment != null) {
            return new HashMap<String, Object>(environment);
        }
        return new HashMap<String, Object>();
    }

    @Override
    public void getInfo(PrintWriter pw) {
        for (final Map.Entry<String, AliasEntry> aliasEntry : aliases.entrySet()) {
            pw.println("<<<<< " + aliasEntry.getKey() + " >>>>>>>>>");
            aliasEntry.getValue().getInfo(pw);
        }
    }

    public Map<String, Object> getReadOnlyContext() {
        if (context != null) {
            return new HashMap<String, Object>(context);
        }
        return new HashMap<String, Object>();
    }

    @Override
    public void setEnvironment(Map<String, Object> environment) {
        this.environment = environment;
    }

    @Override
    protected void init() throws Exception {
        initializeOptions();
        context = getDomainContext();
        server = new Server(httpPort);
        createAndSetThreadPool();

        if (isAuthenticated()) {
            server.setConnectors(new Connector[] { createSimpleConnector(),
                            createSslConnector() });
        } else {
            server.setConnectors(new Connector[] { createSimpleConnector() });
        }

        createAndSetHandlers();

        try {
            logger.debug("starting server");
            server.start();
        } catch (Exception e) {
            logger.error("Jettyserver threw Exception: " + e.getMessage());
            server.destroy();
            throw e;
        }
    }

    private void initializeOptions() {
        final Args args = getArgs();
        String opt = args.getOpt(OPT_HTTP_PORT);
        if (opt != null) {
            httpPort = Integer.parseInt(opt);
        }

        opt = args.getOpt(OPT_HTTPS_PORT);
        if (opt != null) {
            httpsPort = Integer.parseInt(opt);
            if (httpsPort < 1) {
                httpsPort = null;
            }
        }

        opt = args.getOpt(OPT_KEYSTORE);
        if (opt != null) {
            keystore = opt;
        }

        opt = args.getOpt(OPT_KEYSTORE_TYPE);
        if (opt != null) {
            keystoreType = opt;
        }

        opt = args.getOpt(OPT_KEYSTORE_PASSWORD);
        if (opt != null) {
            keystorePassword = opt;
        }

        opt = args.getOpt(OPT_TRUSTSTORE);
        if (opt != null) {
            truststore = opt;
        }

        opt = args.getOpt(OPT_TRUSTSTORE_PASSWORD);
        if (opt != null) {
            trustPassword = opt;
        }

        opt = args.getOpt(OPT_MAX_IDLE_TIME);
        if (opt != null) {
            maxIdleTime = Integer.parseInt(opt);
        }

        opt = args.getOpt(OPT_MAX_THREADS);
        if (opt != null) {
            maxThreads = Integer.parseInt(opt);
        }

        logger.info("{}: {}", OPT_HTTP_PORT, httpPort);
        logger.info("{}: {}", OPT_HTTPS_PORT, httpsPort);
        logger.info("{}: {}", OPT_KEYSTORE_TYPE, keystoreType);
        logger.info("{}: {}", OPT_KEYSTORE, keystore);
        logger.info("{}: {}", OPT_KEYSTORE_PASSWORD, keystorePassword);
        logger.info("{}: {}", OPT_TRUSTSTORE, truststore);
        logger.info("{}: {}", OPT_TRUSTSTORE_PASSWORD, trustPassword);
        logger.info("{}: {}", OPT_MAX_IDLE_TIME, maxIdleTime);
        logger.info("{}: {}", OPT_MAX_THREADS, maxThreads);
    }

    private void createAndSetThreadPool() {
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(maxThreads);
        server.setThreadPool(threadPool);
    }

    private Connector createSimpleConnector() {
        Connector connector = new SelectChannelConnector();
        connector.setPort(httpPort);
        connector.setMaxIdleTime(maxIdleTime);
        return connector;
    }

    private Connector createSslConnector() {
        SslSelectChannelConnector connector = new SslSelectChannelConnector();
        connector.setPort(httpsPort);
        connector.setHost(IPV4_INETADDR_ANY);
        SslContextFactory factory = connector.getSslContextFactory();
        factory.setKeyStore(keystore);
        factory.setKeyStoreType(keystoreType);
        factory.setKeyStorePassword(keystorePassword);
        factory.setTrustStore(truststore);
        factory.setTrustStorePassword(trustPassword);
        factory.setWantClientAuth(true);
        factory.setNeedClientAuth(false);
        return connector;
    }

    private void createAndSetHandlers() {
        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { new HandlerDelegator(aliases),
                        new DefaultHandler(), new RequestLogHandler() });
        server.setHandler(handlers);
    }

    private void shutdownEngines() {
        for (final AliasEntry entry : aliases.values()) {
            entry.shutdown();
        }
    }

    public Server getServer() {
        return server;
    }

    public boolean isAuthenticated() {
        return httpsPort != null;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public Integer getHttpsPort() {
        return httpsPort;
    }
}
