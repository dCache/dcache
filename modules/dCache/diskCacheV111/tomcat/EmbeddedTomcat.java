package diskCacheV111.tomcat ;

import dmg.cells.nucleus.* ;
import  dmg.util.*;

import java.io.PrintWriter ;
import java.util.Date ;

import java.net.URL;

import org.apache.catalina.Connector;
import org.apache.catalina.Context;
import org.apache.catalina.Deployer;
import org.apache.catalina.Engine;
import org.apache.catalina.Host;
import org.apache.catalina.logger.SystemOutLogger;
import org.apache.catalina.realm.MemoryRealm;
import org.apache.catalina.startup.Embedded;
import org.apache.catalina.Container;

import javax.servlet.ServletContext;

public class      EmbeddedTomcat
    extends    CellAdapter 
    implements Runnable {
    
	private String  _cellName = null;
	private Args    _args     = null;
	private CellNucleus _nucleus = null;
	private String _catalinaHome = null;
	private String _catalinaBase = null;

    private Date _creationDate = new Date();

    /**
     * Is naming enabled ?
     */
    protected boolean useNaming = true;

    private String _base = null;
    private Embedded _embedded = null;
    private Host _host = null;
    
    public EmbeddedTomcat(String cellName, String args) 
	{
		super(cellName, args, false);
		_cellName = cellName;
		_args     = getArgs();
		_nucleus  = getNucleus();

		_catalinaHome = _args.getOpt("home");
		if (_catalinaHome != null) {
			System.setProperty("catalina.home", _catalinaHome);
			_catalinaBase = _args.getOpt("base");
			if (_catalinaBase == null) {
				_catalinaBase = _catalinaHome;
				System.setProperty("catalina.base", _catalinaBase);
			}
            say("catalina.home : " + _catalinaHome);
            say("catalina.base : " + _catalinaBase);
			//VP new Thread(this).start();
			_nucleus.newThread(this, "EmbeddedTomcat").start();
 	
			setPrintoutLevel( CellNucleus.PRINT_CELL );
	
			say( "Constructor finished" );
			start();
		} else {
            esay("You must set the 'catalina.home' system property");
		}
    }
    
    public String toString(){
		return "Embedded Tomcat created : "+ _creationDate ;
    }
    
    public void getInfo( PrintWriter pw ){
		super.getInfo( pw ) ;
		pw.println( " Created   : "+_creationDate  )  ;
    }
    
    public void cleanUp(){
		//
		// delay the removal of the cell to 
		// simulate complicated clean up procedures.
		//
		say(" Clean up called ... " ) ;
		try{
			Thread.currentThread().sleep( 2000 ) ;
		} catch( InterruptedException ie ) {
	    
		}
		say( " Done" ) ;
    }
    
    /**
     * Basic Accessor setting the value of the context path
     *
     * @param        path - the path
     */
    public void setBase(String path) {

		_base = path;
    }

    /**
     * Basic Accessor returning the value of the context path
     *
     * @return - the context path
     */
    public String getBase() {

		return _base;
    }

    /**
     * This method Starts the Tomcat server.
     */
    public void startTomcat() throws Exception {

		Engine engine = null;

		// Set the home directory
		// System.setProperty("catalina.home", getBase());

		// Create an embedded server
		_embedded = new Embedded();
		_embedded.setDebug(5);
		_embedded.setLogger(new SystemOutLogger());
		_embedded.setRealm(new MemoryRealm());                 //VP

        // Initialize some naming specific properties
        if (!useNaming) {
            System.setProperty("catalina.useNaming", "false");
        } else {
            System.setProperty("catalina.useNaming", "true");
            String value = "org.apache.naming";
            String oldValue =
                System.getProperty(javax.naming.Context.URL_PKG_PREFIXES);
            if (oldValue != null) {
                value = oldValue + ":" + value;
            }
            System.setProperty(javax.naming.Context.URL_PKG_PREFIXES, value);
            System.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY,
                               "org.apache.naming.java.javaURLContextFactory");
        }

		// Create an engine
		engine = _embedded.createEngine();
		engine.setDefaultHost("localhost");

		// Create a default virtual host
		_host = _embedded.createHost("localhost", getBase() + "/webapps");
		engine.addChild(_host);

		// Create the ROOT context
		Context context = _embedded.createContext("", getBase() + "/webapps/ROOT");
		_host.addChild(context);
    
		// Create the examples context
		Context examplesContext = _embedded.createContext("/examples", getBase() + "/webapps/examples");
		_host.addChild(examplesContext);
    
		// Create the axis context
		Context axisContext = _embedded.createContext("/axis", getBase() + "/webapps/axis");
		_host.addChild(axisContext);
    
		// Install the assembled container hierarchy
		_embedded.addEngine(engine);

		// Assemble and install a default HTTP connector
		Connector connector = _embedded.createConnector(null, 8080, false);
		_embedded.addConnector(connector);

		//VP - Pass the reference to embedded tomcat to allow to the servlets to use cell interface
		ServletContext servletContext = examplesContext.getServletContext();
		servletContext.setAttribute("parent", this);
		//VP


		// Start the embedded server
		_embedded.start();
    }

    /**
     * This method Stops the Tomcat server.
     */
    public void stopTomcat() throws Exception {

		// Stop the embedded server
		_embedded.stop();
    }

    /**
     * Registers a WAR
     *
     * @param contextPath - the context path under which the
     *               application will be registered
     * @param url - the URL of the WAR file to be registered.
     */
    public void registerWAR(String contextPath, URL url) throws Exception {

		if ( contextPath == null ) {

			throw new Exception("Invalid Path : " + contextPath);
		}
		String displayPath = contextPath;
		if( contextPath.equals("/") ) {

			contextPath = "";
		}

		if ( url == null ) {

			throw new Exception("Invalid WAR : " + url);
		}

		Deployer deployer = (Deployer)_host;
		Context context = deployer.findDeployedApp(contextPath);

		if (context != null) {

			throw new Exception("Context " + contextPath + " already Exists!");
		}
		deployer.install(contextPath, url);
    }

    /**
     * removes a WAR
     *
     * @param contextPath - the context path to be removed
     */
    public void unregisterWAR(String contextPath) throws Exception {

		Context context = _host.map(contextPath);
		if ( context != null ) {

			_embedded.removeContext(context);
		}
		else {

			throw new Exception("Context does not exist for named path : "
								+ contextPath);
		}
    }

    //
    // runnable interface
    //
    
    public void run()
    {
		try {

			setBase(_catalinaBase);
	    
			startTomcat();

			while (!Thread.currentThread().interrupted()) {
				Thread.currentThread().sleep(1000);
			}
		} catch (Exception ee ) {
			say("Embedded Tomcat interrupted due to : "+ee ) ;
			try {
				stopTomcat();
			} catch (Exception ste ) {
				;
			}
		}
    }  
}
