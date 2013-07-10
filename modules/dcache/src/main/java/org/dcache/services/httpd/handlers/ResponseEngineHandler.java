package org.dcache.services.httpd.handlers;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URISyntaxException;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;

import org.dcache.services.httpd.HttpServiceCell;
import org.dcache.services.httpd.util.StandardHttpRequest;

import static com.google.common.base.Preconditions.checkNotNull;

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
        checkNotNull(engine);
        try {
            HttpRequest proxy = new StandardHttpRequest(request, response);
            engine.queryUrl(proxy);
            proxy.getPrintWriter().flush();
        } catch (HttpException e) {
            response.sendError(e.getErrorCode(), e.getMessage());
        } catch (URISyntaxException e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    e.getMessage());
        }
    }

    public HttpResponseEngine getEngine() {
        return engine;
    }

    public CellInfoProvider getCellInfoProvider() {
        if (engine instanceof CellInfoProvider) {
            return (CellInfoProvider) engine;
        } else {
            return null;
        }
    }

    public void initialize(HttpServiceCell cell) throws Exception {
        final Class<? extends HttpResponseEngine> c = Class.forName(className).asSubclass(HttpResponseEngine.class);

        /*
         * find constructor: (a) <init>(CellNucleus nucleus, String [] args) (b)
         * <init>(String [] args) (c) <init>()
         */
        try {
            Class<?>[] argsClass = new Class<?>[2];
            argsClass[0] = CellEndpoint.class;
            argsClass[1] = String[].class;
            Constructor<? extends HttpResponseEngine> constr = c.getConstructor(argsClass);
            Object[] args = new Object[2];
            args[0] = cell;
            args[1] = this.args;
            engine = constr.newInstance(args);
        } catch (final Exception e) {
            try {
                Class<?>[] argsClass = new Class<?>[1];
                argsClass[0] = String[].class;
                Constructor<? extends HttpResponseEngine> constr = c.getConstructor(argsClass);
                Object[] args = new Object[1];
                args[0] = this.args;
                engine = constr.newInstance(args);
            } catch (final Exception ee) {
                Class<?>[] argsClass = new Class<?>[0];
                Constructor<? extends HttpResponseEngine> constr = c.getConstructor(argsClass);
                engine = constr.newInstance();
            }
        }
        cell.addCommandListener(engine);
        if (engine instanceof EnvironmentAware) {
            ((EnvironmentAware) engine).setEnvironment(cell.getEnvironment());
        }
        engine.startup();
    }
}
