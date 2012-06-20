package org.dcache.services.httpd.handlers;

import java.io.IOException;
import java.lang.reflect.Constructor;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.dcache.services.httpd.HttpServiceCell;
import org.dcache.services.httpd.util.StandardHttpRequest;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;

/**
 * Wraps calls to {@link HttpResponseEngine} aliases with the Jetty handler API.
 *
 * @author arossi
 */
public class ResponseEngineHandler extends AbstractHandler {

    private final String className;
    private final String[] args;
    private HttpResponseEngine engine;

    public ResponseEngineHandler(String className, String[] args) {
        this.className = className;
        this.args = args;
    }

    @Override
    public void handle(String target, Request baseRequest,
                    HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException {
        if (engine != null) {
            HttpRequest proxy = null;
            try {
                proxy = new StandardHttpRequest(request, response);
                engine.queryUrl(proxy);
            } catch (final Exception e) {
                String error = "HttpResponseEngine ("
                                + engine.getClass().getCanonicalName()
                                + ") is broken, please report this to sysadmin.";
                Exception httpException
                    = new HttpException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                                        error);
                throw new ServletException(httpException);
            } finally {
                if (proxy != null) {
                    proxy.getPrintWriter().flush();
                }
            }
        }
    }

    public HttpResponseEngine getEngine() {
        return engine;
    }

    public void initialize(HttpServiceCell cell) throws Exception {
        final Class c = Class.forName(className);

        /*
         * find constructor: (a) <init>(CellNucleus nucleus, String [] args) (b)
         * <init>(String [] args) (c) <init>()
         */
        try {
            Class[] argsClass = new Class[2];
            argsClass[0] = CellNucleus.class;
            argsClass[1] = String[].class;
            Constructor constr = c.getConstructor(argsClass);
            Object[] args = new Object[2];
            args[0] = cell.getNucleus();
            args[1] = this.args;
            engine = (HttpResponseEngine) constr.newInstance(args);
        } catch (final Exception e) {
            try {
                Class[] argsClass = new Class[1];
                argsClass[0] = String[].class;
                Constructor constr = c.getConstructor(argsClass);
                Object[] args = new Object[1];
                args[0] = this.args;
                engine = (HttpResponseEngine) constr.newInstance(args);
            } catch (final Exception ee) {
                Class[] argsClass = new Class[0];
                Constructor constr = c.getConstructor(argsClass);
                engine = (HttpResponseEngine) constr.newInstance(new Object[0]);
            }
        }
        cell.addCommandListener(engine);
        if (engine instanceof EnvironmentAware) {
            ((EnvironmentAware) engine).setEnvironment(cell.getEnvironment());
        }
        engine.startup();
    }
}
