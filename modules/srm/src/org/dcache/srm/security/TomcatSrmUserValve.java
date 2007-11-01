/* this code is the Fermilab modification of the CertificateValve class
 * from the java globus cog kit 1.1 
 * 
 */

/*
This file is licensed under the terms of the Globus Toolkit Public
License, found at http://www.globus.org/toolkit/download/license.html.
*/
package org.dcache.srm.security;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import org.apache.catalina.Context;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.Logger;
import org.apache.catalina.Request;
import org.apache.catalina.Response;
import org.apache.catalina.ValveContext;
//RequestWrapper is deprecated
//import org.apache.catalina.connector.RequestWrapper;
import org.apache.catalina.deploy.LoginConfig;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.catalina.util.StringManager;
import org.apache.catalina.valves.ValveBase;
import org.apache.catalina.valves.Constants;


public final class TomcatSrmUserValve extends ValveBase implements Lifecycle {

    // ----------------------------------------------------- Instance Variables
   public static final String SECURITY_CONTEXT="SECURITY_CONTEXT";
   public static final String CLIENT_SOCKET = "CLIENT_SOCKET";

   protected boolean certificates;
    /**
     * The debugging detail level for this component.
     */
    protected int debug = 0;


    /**
     * The descriptive information related to this implementation.
     */
    protected static final String info =
        "org.dcache.srm.security.TomcatSrmUserValve/1.0";


    /**
     * The lifecycle event support for this component.
     */
    protected LifecycleSupport lifecycle = new LifecycleSupport(this);


    /**
     * The StringManager for this package.
     */
    protected static StringManager sm =
        StringManager.getManager(Constants.Package);


    /**
     * Has this component been started yet?
     */
    protected boolean started = false;


    // ------------------------------------------------------------- Properties


    /**
     * Return the debugging detail level for this component.
     */
    public int getDebug() {

        return (this.debug);

    }


    /**
     * Set the debugging detail level for this component.
     */
    public void setDebug(int debug) {

        this.debug = debug;

    }


    /**
     * Return descriptive information about this Valve implementation.
     */
    public String getInfo() {

        return (info);

    }


    // --------------------------------------------------------- Public Methods


    /**
     * Expose the certificates chain if one was included on this request.
     *
     * @param request The servlet request to be processed
     * @param response The servlet response to be created
     * @param context The valve context used to invoke the next valve
     *  in the current processing pipeline
     *
     * @exception IOException if an input/output error occurs
     * @exception ServletException if a servlet error occurs
     */
    public void invoke(Request request, Response response,
                       ValveContext context)
        throws IOException, ServletException {

        // Identify the underlying request if this request was wrapped
        Request actual = request;
        //RequestWrapper is deprecated
        //while (actual instanceof RequestWrapper)
        //{
        //    if (debug >= 2)
        //        log("Unwrapping request "+actual);
        //    actual = ((RequestWrapper) actual).getWrappedRequest();
        //}
        
        if (debug >= 2)
            log("Processing request "+request);

        // Verify the existence of a certificate chain if appropriate
        if (certificates)
            verify(request, actual);

        // Expose the certificate chain if appropriate
        expose(request, actual);
        ServletRequest srequest = request.getRequest();
        // Invoke the next Valve in our Pipeline
        try {
	        context.invokeNext(request, response);
        } catch(Exception e) {
           e.printStackTrace();
        }

    }


    // ------------------------------------------------------ Lifecycle Methods


    /**
     * Add a LifecycleEvent listener to this component.
     *
     * @param listener The listener to add
     */
    public void addLifecycleListener(LifecycleListener listener) {
        lifecycle.addLifecycleListener(listener);
    }


    /**
     * Remove a LifecycleEvent listener from this component.
     *
     * @param listener The listener to remove
     */
    public void removeLifecycleListener(LifecycleListener listener) {
        lifecycle.removeLifecycleListener(listener);
    }
    
    public LifecycleListener[] findLifecycleListeners() {
	return new LifecycleListener[] {};
    }


    /**
     * Prepare for the beginning of active use of the public methods of this
     * component.  This method should be called before any of the public
     * methods of this component are utilized.  It should also send a
     * LifecycleEvent of type START_EVENT to any registered listeners.
     *
     * @exception IllegalStateException if this component has already been
     *  started
     * @exception LifecycleException if this component detects a fatal error
     *  that prevents this component from being used
     */
    public void start() throws LifecycleException {

        // Validate and update our current component state
        if (started)
            throw new LifecycleException
                (sm.getString("certificatesValve.alreadyStarted"));
        started = true;
        if (debug >= 1)
            log("Starting");

        // Check what type of authentication (if any) we are doing
        certificates = false;
        if (container instanceof Context) {
            Context context = (Context) container;
            LoginConfig loginConfig = context.getLoginConfig();
            if (loginConfig != null) {
                String authMethod = loginConfig.getAuthMethod();
                if ("CLIENT-CERT".equalsIgnoreCase(authMethod))
                    certificates = true;
            }
        }

        // Notify our interested LifecycleListeners
        lifecycle.fireLifecycleEvent(Lifecycle.START_EVENT, null);

    }


    /**
     * Gracefully terminate the active use of the public methods of this
     * component.  This method should be the last one called on a given
     * instance of this component.  It should also send a LifecycleEvent
     * of type STOP_EVENT to any registered listeners.
     *
     * @exception IllegalStateException if this component has not been started
     * @exception LifecycleException if this component detects a fatal error
     *  that needs to be reported
     */
    public void stop() throws LifecycleException {

        // Validate and update our current component state
        if (!started)
            throw new LifecycleException
                (sm.getString("certificatesValve.notStarted"));
        lifecycle.fireLifecycleEvent(Lifecycle.STOP_EVENT, null);
        started = false;
        if (debug >= 1)
            log("Stopping");

        certificates = false;

    }


    // ------------------------------------------------------ Protected Methods


    /**
     * Expose the certificate chain for this request, if there is one.
     *
     * @param request The possibly wrapped Request being processed
     * @param actual The actual underlying Request object
     */
    protected void expose(Request request, Request actual) {

	if (debug >= 2) {
	    log(" expose: request="+request+" actual="+actual);
	}
        // Ensure that this request came in on an SSLSocket
        if (actual.getSocket() == null) {
            return;
        }
        
        if (!(actual.getSocket() instanceof 
            TomcatGSISocket)) {
            return;
        }

	if (debug >= 2) {
	    log(" expose: It's a TomcatGSISocket");
	}
	
        TomcatGSISocket socket = 
            (TomcatGSISocket) actual.getSocket();
	
	Object tmp =socket.getContext();
        
        if (debug >= 2) {
            log(" expose: tmp is "+tmp);
        }
       if(tmp != null)
       {
            if (debug >= 1) {
                log(" expose: srmUser is "+tmp);
            }
            request.getRequest().setAttribute(SECURITY_CONTEXT,
                                              tmp);
            request.getRequest().setAttribute(CLIENT_SOCKET,socket); 
       }
    }

    /**
     * Log a message on the Logger associated with our Container (if any).
     *
     * @param message Message to be logged
     */
    protected void log(String message) {
	if (container != null && container.getLogger() != null) {
	    Logger logger = container.getLogger();
            logger.log("TomcatSrmUserValve[" + container.getName() + "]: " +
                       message);
	} else {
            System.out.println("CertificatesValve: " + message);
	}
    }

    /**
     * Log a message on the Logger associated with our Container (if any).
     *
     * @param message Message to be logged
     * @param throwable Associated exception
     */
    protected void log(String message, Throwable throwable) {
	if (container != null && container.getLogger() != null) {
	    Logger logger = container.getLogger();
            logger.log("TomcatSrmUserValve[" + container.getName() + "]: " +
                       message, throwable);
        } else {
            System.out.println("TomcatSrmUserValve: " + message);
            throwable.printStackTrace(System.out);
        }
    }

    /**
     * Verify that a client certificate chain exists if our web application
     * is doing client certificate authentication.
     *
     * @param request The possibly wrapped Request being processed
     * @param actual The actual underlying Request object
     */
    protected void verify(Request request, Request actual) {
    }


}

