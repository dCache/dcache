/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */
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

        Class[] argsClass = null;
        Object[] args = null;
        Constructor constr = null;
        /*
         * find constructor: (a) <init>(CellNucleus nucleus, String [] args) (b)
         * <init>(String [] args) (c) <init>()
         */
        try {
            argsClass = new Class[2];
            argsClass[0] = CellNucleus.class;
            argsClass[1] = String[].class;
            constr = c.getConstructor(argsClass);
            args = new Object[2];
            args[0] = cell.getNucleus();
            args[1] = this.args;
            engine = (HttpResponseEngine) constr.newInstance(args);
        } catch (final Exception e) {
            try {
                argsClass = new Class[1];
                argsClass[0] = String[].class;
                constr = c.getConstructor(argsClass);
                args = new Object[1];
                args[0] = this.args;
                engine = (HttpResponseEngine) constr.newInstance(args);
            } catch (final Exception ee) {
                argsClass = new Class[0];
                constr = c.getConstructor(argsClass);
                args = new Object[0];
                engine = (HttpResponseEngine) constr.newInstance(args);
            }
        }
        cell.addCommandListener(engine);
        if (engine instanceof EnvironmentAware) {
            ((EnvironmentAware) engine).setEnvironment(cell.getEnvironment());
        }
        engine.startup();
    }
}
