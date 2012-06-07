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
import java.util.Map;
import java.util.StringTokenizer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.dcache.services.httpd.exceptions.OnErrorException;
import org.dcache.services.httpd.util.AliasEntry;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.util.HttpBasicAuthenticationException;
import dmg.util.HttpException;

/**
 * Responsible for parsing the request to find the correct alias type and
 * passing the context to the alias handler.
 *
 * @author arossi
 */
public class HandlerDelegator extends AbstractHandler {
    private static final Logger logger = LoggerFactory.getLogger(HandlerDelegator.class);

    private Map<String, AliasEntry> aliases;

    public HandlerDelegator(Map<String, AliasEntry> aliases) {
        this.aliases = aliases;
    }

    @Override
    public void handle(String target, Request baseRequest,
                    HttpServletRequest request, HttpServletResponse response)
                    throws IOException {

        final String uri = request.getRequestURI();
        final String alias = extractAlias(uri);

        logger.info("handle {}, {}", uri, alias);

        AliasEntry entry = null;
        Handler handler = null;
        String alternate = null;

        try {
            entry = aliases.get(alias);

            if (entry == null) {
                entry = aliases.get("<default>");
            }

            if (entry == null) {
                throw new HttpException(HttpServletResponse.SC_NOT_FOUND,
                                "Alias not found : " + alias);
            }

            logger.debug("alias: {}, entry {}", alias, entry);

            /*
             * The exclusion of POST used to be absolute; we allow it now
             * only on webapps.
             */
            if (!request.getMethod().equals("GET")
                            && !entry.getType().equals(AliasEntry.TYPE_WEBAPP)) {
                throw new HttpException(HttpServletResponse.SC_NOT_IMPLEMENTED,
                                "Method not implemented: "
                                                + request.getMethod());
            }

            /*
             * check for overwritten alias
             */
            if ((alternate = entry.getOverwrite()) != null) {
                logger.debug("handle, overwritten alias: {}", alternate);
                final AliasEntry overwrittenEntry = aliases.get(alternate);
                if (overwrittenEntry != null) {
                    entry = overwrittenEntry;
                }
                logger.debug("handle, alias {}, entry {}", alternate, entry);
            }

            handler = entry.getHandler();
            logger.debug("got handler: {}", handler);
            if (handler != null) {
                handler.handle(target, baseRequest, request, response);
            }

        } catch (final Exception e) {
            if (entry != null && e.getCause() instanceof OnErrorException) {
                if ((alternate = entry.getOnError()) != null) {
                    logger.debug("handle, onError alias: {}", alternate);
                    final AliasEntry overwrittenEntry = aliases.get(alternate);
                    if (overwrittenEntry != null) {
                        entry = overwrittenEntry;
                        logger.debug("handle, alias {}, entry {}", alternate,
                                        entry);
                        handler = entry.getHandler();
                        if (handler != null) {
                            try {
                                handler.handle(target, baseRequest, request,
                                                response);
                            } catch (final ServletException t) {
                                handleException(t, uri, response);
                            }
                        }
                    } else {
                        handleException(new HttpException(
                                        HttpServletResponse.SC_NOT_FOUND,
                                        "Not found : "
                                                        + entry.getSpecificString()),
                                        uri, response);
                    }
                }
            } else {
                handleException(e, uri, response);
            }
        }

        logger.info("Finished");
    }

    private static String extractAlias(String requestURI) {
        final StringTokenizer st = new StringTokenizer(requestURI, "/");
        String alias = "<home>";
        if (st.hasMoreTokens()) {
            alias = st.nextToken();
        }
        return alias;
    }

    private static void handleException(Exception e, String uri,
                    HttpServletResponse response) {
        if (e instanceof ServletException) {
            final Throwable cause = e.getCause();
            if (cause instanceof HttpException) {
                printHttpException((HttpException) cause, response);
            }
            logger.warn("Problem with {}: {}", uri, e.getMessage());
        } else if (e instanceof HttpException) {
            printHttpException((HttpException) e, response);
            logger.warn("Problem with {}: {}", uri, e.getMessage());
        } else {
            printHttpException(new HttpException(
                            HttpServletResponse.SC_BAD_REQUEST,
                            "Bad Request : " + e), response);
            logger.warn("Problem in HttpServiceCellHandler: {}", e);
        }
    }

    private static void printHttpException(HttpException exception,
                    HttpServletResponse response) {
        try {
            if (exception instanceof HttpBasicAuthenticationException) {
                final String realm
                    = ((HttpBasicAuthenticationException) exception).getRealm();
                response.setHeader("WWW-Authenticate", "Basic realm=\"" + realm
                                + "\"");
            }
            response.sendError(exception.getErrorCode(), exception.getMessage());
        } catch (final IOException e) {
            logger.warn("Failed to send reply: {}", e.getMessage());
        }
    }
}
