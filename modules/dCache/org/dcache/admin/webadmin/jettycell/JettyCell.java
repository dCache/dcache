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

/**
 *
 * @author jans 28-10-2009
 */
public class JettyCell extends AbstractCell {

//    the context where the Jettycell can be found in order to reference it
//    from within the webapps
    public static final String JETTYCELL_NAMING_CONTEXT = "jettycell";
    public static final int MAX_IDLE_TIME = 30000;
//    args entry to the webapps path
    public static final String WEBAPPS_PATH = "webapps_Path";
//    args entry to the jetty port
    public static final String JETTY_PORT = "jetty_Port";
//    max Threads for the Jetty Server maybe later configurable
    public static final int MAX_THREADS = 100;
    public static final String WEBAPP_CONTEXT = "/webadmin";
    private static final String WEBDEFAULT_XML = "/webdefault.xml";
    private static final Logger _log = LoggerFactory.getLogger(JettyCell.class);
//    path to the .war files with the webapps, will be set by the batch file
    private final String _webappsPath;
//    defaulted with 8080
    private int _jettyPort = 8080;
//    URL to the ressources folder of jetty (will be put inside of dcache.jar)
//    dont know how to get rid of the hardcoding, should be
//    package of class+/resources
    private final URL _resourceURL = JettyCell.class.getResource("/org/dcache/admin/webadmin/jettycell/resources");
    private Server _server;

    /**
     * @param cellName Name of the Cell
     * @param args the arguments delivered when the BatchCell creates the Jettycell
     * these are needed:
     * Jettycell.WEBAPPS_PATH - path to the webapps folder with the .war file
     */
    public JettyCell(String cellName, String args) throws InterruptedException,
            ExecutionException {
        super(cellName, args);
        _log.debug("constructor entered");
        _webappsPath = getArgs().getOpt(WEBAPPS_PATH);
        _jettyPort = Integer.parseInt(getArgs().getOpt(JETTY_PORT));
        doInit();
        _log.debug("Constructor called");
    }

    @Override
    protected void init() throws Exception {
        super.init();
        _log.debug("initialising");
        this.createJetty();
    }

    private void createJetty() {

        _server = new Server(_jettyPort);
        createAndSetThreadPool();
        _server.setConnectors(new Connector[]{initConnector()});
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

    private Connector initConnector() {
        Connector connector = new SelectChannelConnector();
        connector.setPort(_jettyPort);
        connector.setMaxIdleTime(MAX_IDLE_TIME);
        return connector;
    }

    private void createAndSetHandlers() {
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{(Handler) createWebappContext(), new DefaultHandler(), requestLogHandler});
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
