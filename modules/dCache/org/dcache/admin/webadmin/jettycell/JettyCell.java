package org.dcache.admin.webadmin.jettycell;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import javax.naming.NamingException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.dcache.cells.AbstractCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import javax.naming.InitialContext;
import org.dcache.cells.Option;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;

/**
 *
 * @author jans 28-10-2009
 */
public class JettyCell extends AbstractCell {

//    where to find the jettyhost
    public static final String IPV4_INETADDR_ANY = "0.0.0.0";
//    the context where the Jettycell can be found in order to reference it
//    from within the webapps
    public static final String JETTYCELL_NAMING_CONTEXT = "jettycell";
    public static final int MAX_IDLE_TIME = 30000;
//    max Threads for the Jetty Server maybe later configurable
    public static final int MAX_THREADS = 100;
    public static final String WEBAPP_CONTEXT = "/webadmin";
    private static final String WEBDEFAULT_XML = "/webdefault.xml";
    private static final Logger _log = LoggerFactory.getLogger(JettyCell.class);
    @Option(name = "webappsPath",
    description = "path to the .war files with the webapps",
    required = true)
    private String _webappsPath;
    @Option(name = "authenticated",
    description = "webapp runs in authenticated mode",
    required = true)
    private boolean _authenticatedMode;
    @Option(name = "httpPort",
    description = "Port where Jetty is connectable via http",
    required = true)
    private int _httpPort;
    @Option(name = "httpsPort",
    description = "Port where Jetty is connectable via https",
    required = false)
    private int _httpsPort;
    @Option(name = "keystore",
    description = "The keystore for SSL",
    required = false)
    private String _keystore;
    @Option(name = "keystoreType",
    description = "The keystoreType for SSL",
    required = false)
    private String _keystoreType;
    @Option(name = "keystorePassword",
    description = "The keystore password",
    log = false,
    required = false)
    private String _keystorePassword;
    @Option(name = "truststore",
    description = "The truststore for SSL",
    required = false)
    private String _truststore;
    @Option(name = "truststorePassword",
    description = "The truststore password",
    log = false,
    required = false)
    private String _trustPassword;
//    the GID a user has to have to be considered an Admin of webadmininterface
    @Option(name = "admin-GID",
    description = "admin GID for webadmininterface",
    required = false)
    private int _adminGid;
//    FIXME with new gplazma this should be known by gplazma and not be needed
//    anymore
    @Option(name = "kpwd-file",
    description = "path to the kpwd-file",
    required = false)
    private String _kpwdFile;
    @Option(name = "dCacheInstanceName",
    description = "The dCache-Instance Name",
    required = false)
    private String _dcacheName;
    @Option(name = "loginBrokerName",
    description = "LoginBrokers Name",
    required = true)
    private String _loginBrokerName;
    @Option(name = "pnfsManagerName",
    description = "Name of pnfsManager cell",
    required = true)
    private String _pnfsManagerName;
    @Option(name = "poolManagerName",
    description = "Name of poolManager cell",
    required = true)
    private String _poolManagerName;
    @Option(name = "gPlazmaName",
    description = "Name of gPlazma cell",
    required = true)
    private String _gPlazmaName;
    @Option(name = "collectorTimeout",
    description = "timeout for the collectors",
    required = true)
    private long _collectorTimeout;
    @Option(name = "transfersCollectorUpdate",
    description = "Update interval of transferscollector",
    required = true)
    private long _transfersCollectorUpdate;
//    URL to the ressources folder of jetty (will be put inside of dcache.jar)
//    dont know how to get rid of the hardcoding, should be
//    package of class+/resources
    private final URL _resourceURL = JettyCell.class.getResource("/org/dcache/admin/webadmin/jettycell/resources");
    private Server _server;

    /**
     * @param cellName Name of the Cell
     * @param args the arguments delivered when the BatchCell creates the Jettycell
     */
    public JettyCell(String cellName, String args) throws InterruptedException,
            ExecutionException {
        super(cellName, args);
        _log.debug("constructor entered");
        doInit();
        _log.debug("Constructor successfully called");
    }

    @Override
    protected void init() throws Exception {
        super.init();
        _log.debug("initialising");
        this.createJetty();
    }

    private void createJetty() {
        _server = new Server(_httpPort);
        createAndSetThreadPool();
        if (_authenticatedMode) {
            _server.setConnectors(new Connector[]{createSimpleConnector(), createSslConnector()});
        } else {
            _server.setConnectors(new Connector[]{createSimpleConnector()});
        }
        createAndSetHandlers();
        try {
//            make the cell known for the webapps
            InitialContext namingContext = new InitialContext();
            namingContext.bind(JETTYCELL_NAMING_CONTEXT, this);
            _log.debug("starting server");
            _server.start();
        } catch (NamingException ex) {
            _log.error("Namingcontext couldn't initialise (jndi-lib in classpath?) " +
                    ex.getMessage());
        } catch (Exception e) {
            _log.error("Jettyserver threw Exception: " + e.getMessage());
            _server.destroy();
        }
    }

    private void createAndSetThreadPool() {
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(MAX_THREADS);
        _server.setThreadPool(threadPool);
    }

    private Connector createSimpleConnector() {
        Connector connector = new SelectChannelConnector();
        connector.setPort(_httpPort);
        connector.setMaxIdleTime(MAX_IDLE_TIME);
        return connector;
    }

    private Connector createSslConnector() {
        SslSelectChannelConnector connector = new SslSelectChannelConnector();
        connector.setPort(_httpsPort);
        connector.setHost(IPV4_INETADDR_ANY);
        connector.setKeystore(_keystore);
        connector.setKeystoreType(_keystoreType);
        connector.setPassword(_keystorePassword);
        connector.setTruststore(_truststore);
        connector.setTrustPassword(_trustPassword);
        connector.setWantClientAuth(true);
        connector.setNeedClientAuth(false);
        return connector;
    }

    private void createAndSetHandlers() {
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{(Handler) createWebappContext(),
                    new LegacyForwardHandler(), new DefaultHandler(),
                    requestLogHandler});
        _server.setHandler(handlers);
    }

    private WebAppContext createWebappContext() {
        WebAppContext webappContext = new WebAppContext();
        _log.debug(_resourceURL.toExternalForm());
        webappContext.setDefaultsDescriptor(_resourceURL.toExternalForm() + WEBDEFAULT_XML);
        webappContext.setContextPath(WEBAPP_CONTEXT);
        webappContext.setWar(_webappsPath + WEBAPP_CONTEXT + ".war");
        webappContext.setExtractWAR(true);
        return webappContext;
    }

    public int getAdminGid() {
        return _adminGid;
    }

    public String getKpwdFile() {
        return _kpwdFile;
    }

    public String getDcacheName() {
        return _dcacheName;
    }

    public int getHttpPort() {
        return _httpPort;
    }

    public int getHttpsPort() {
        return _httpsPort;
    }

    public boolean isAuthenticatedMode() {
        return _authenticatedMode;
    }

    public long getCollectorTimeout() {
        return _collectorTimeout;
    }

    public String getLoginBrokerName() {
        return _loginBrokerName;
    }

    public String getPnfsManagerName() {
        return _pnfsManagerName;
    }

    public String getPoolManagerName() {
        return _poolManagerName;
    }

    public String getGPlazmaName() {
        return _gPlazmaName;
    }

    public long getTransfersCollectorUpdate() {
        return _transfersCollectorUpdate;
    }

    private class LegacyForwardHandler extends AbstractHandler {

        private final String INFO_CONTEXT = "/info";
        private final String CELLINFO_CONTEXT = "/cellInfo";
        private final String QUEUEINFO_CONTEXT = "/queueInfo";
        private final String USAGEINFO_CONTEXT = "/usageInfo";
        private final String POOLGROUPS_CONTEXT = "/pools";
        private final String WEBADMIN_INFO_CONTEXT = "/webadmin/info?statepath=";
        private final String WEBADMIN_CELLINFO_CONTEXT = "/webadmin/cellinfo";
        private final String WEBADMIN_QUEUEINFO_CONTEXT = "/webadmin/queueinfo";
        private final String WEBADMIN_USAGEINFO_CONTEXT = "/webadmin/usageinfo";
        private final String WEBADMIN_POOLGROUPS_CONTEXT = "/webadmin/poolgroups";
        private final int CONTEXT_INDEX = 1;
        private Map<String, String> legacyContextToNewContext = new HashMap<String, String>();

        @Override
        protected void doStart() throws Exception {
            super.doStart();
            initContextMapping();
        }

        private void initContextMapping() {
            _log.debug("init of urlmap");
            legacyContextToNewContext.put(INFO_CONTEXT, WEBADMIN_INFO_CONTEXT);
            legacyContextToNewContext.put(CELLINFO_CONTEXT, WEBADMIN_CELLINFO_CONTEXT);
            legacyContextToNewContext.put(QUEUEINFO_CONTEXT, WEBADMIN_QUEUEINFO_CONTEXT);
            legacyContextToNewContext.put(USAGEINFO_CONTEXT, WEBADMIN_USAGEINFO_CONTEXT);
            legacyContextToNewContext.put(POOLGROUPS_CONTEXT, WEBADMIN_POOLGROUPS_CONTEXT);
        }

        @Override
        public void handle(String target, Request baseRequest,
                HttpServletRequest request, HttpServletResponse response) throws
                IOException, ServletException {
//          redirect Root Context To Webadmin
            if (target.equals("/")) {
                response.sendRedirect("/webadmin");
                return;
            }
            String[] contexts = target.split("/");
            if (isContextLegacyOne(contexts)) {
                _log.debug("target: {}", target);
                String legacyContext = "/" + contexts[CONTEXT_INDEX];
                String webadminContext = legacyContextToNewContext.get(legacyContext);
                StringBuffer targetUrl = new StringBuffer(target);
                int i = targetUrl.indexOf(legacyContext);
                String newUrl = targetUrl.replace(
                        i, i + legacyContext.length(), webadminContext).toString();
                _log.debug("redirected to: {}", newUrl);
                response.sendRedirect(newUrl);
            }
        }

        private boolean isContextLegacyOne(String[] contexts) {
            if (contexts.length > CONTEXT_INDEX) {
                return legacyContextToNewContext.containsKey("/" + contexts[CONTEXT_INDEX]);
            }
            return false;
        }
    }
}
