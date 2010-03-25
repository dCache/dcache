package org.dcache.admin.webadmin.jettycell;

import java.util.concurrent.ExecutionException;
import javax.naming.NamingException;

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
import javax.naming.InitialContext;
import org.dcache.cells.Option;
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
    @Option(name = "httpPort",
    description = "Port where Jetty is connectable via http",
    required = true)
    private int _httpPort;
    @Option(name = "httpsPort",
    description = "Port where Jetty is connectable via https",
    required = true)
    private int _httpsPort;
    @Option(name = "keystore",
    description = "The keystore for SSL",
    required = true)
    private String _keystore;
    @Option(name = "keystoreType",
    description = "The keystoreType for SSL",
    required = true)
    private String _keystoreType;
    @Option(name = "keystorePassword",
    description = "The keystore password",
    log = false,
    required = true)
    private String _keystorePassword;
    @Option(name = "truststore",
    description = "The truststore for SSL",
    required = true)
    private String _truststore;
    @Option(name = "truststorePassword",
    description = "The truststore password",
    log = false,
    required = true)
    private String _trustPassword;
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
        _server.setConnectors(new Connector[]{createSimpleConnector(), createSslConnector()});
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
                    new DefaultHandler(), requestLogHandler});
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
}
