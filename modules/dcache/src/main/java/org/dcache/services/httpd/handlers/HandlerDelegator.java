package org.dcache.services.httpd.handlers;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import dmg.util.HttpBasicAuthenticationException;
import dmg.util.HttpException;

import org.dcache.services.httpd.exceptions.OnErrorException;
import org.dcache.services.httpd.util.AliasEntry;

/**
 * Responsible for parsing the request to find the correct alias type and
 * passing the context to the alias handler.
 *
 * @author arossi
 */
public class HandlerDelegator extends AbstractHandler {
    private static final Logger logger
        = LoggerFactory.getLogger(HandlerDelegator.class);
    private static final Splitter PATH_SPLITTER
        = Splitter.on('/').omitEmptyStrings();

    private static String extractAlias(String requestURI)
    {
        return Iterables.getFirst(PATH_SPLITTER.split(requestURI), "<home>");
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
        } else if (e instanceof RuntimeException) {
            printHttpException(new HttpException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                                                "Internal problem processing request"), response);
            logger.warn("Bug found, please report it", e);
        } else {
            printHttpException(new HttpException(HttpServletResponse.SC_BAD_REQUEST,
                                                "Bad Request : " + e), response);
            logger.warn("Problem in HttpServiceCellHandler: {}", e.getMessage());
        }
    }

    private static void printHttpException(HttpException exception,
                    HttpServletResponse response) {
        try {
            if (exception instanceof HttpBasicAuthenticationException) {
                final String realm
                    = ((HttpBasicAuthenticationException) exception).getRealm();
                response.setHeader("WWW-Authenticate", "Basic realm=\""
                                    + realm + "\"");
            }
            response.sendError(exception.getErrorCode(), exception.getMessage());
        } catch (final IOException e) {
            logger.warn("Failed to send reply: {}", e.getMessage());
        }
    }

    private final Map<String, AliasEntry> aliases = new ConcurrentHashMap<>();

    @Override
    public void handle(String target, Request baseRequest,
                    HttpServletRequest request, HttpServletResponse response)
                    throws IOException {

        String uri = null;
        String alias;
        AliasEntry entry = null;

        try {
            uri = request.getRequestURI();
            alias = extractAlias(uri);

            logger.debug("handle {}, {}", uri, alias);

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
             * The exclusion of POST used to be absolute; we allow it now only
             * on webapps.
             */
            if (!request.getMethod().equals("GET") && entry.getType() != AliasEntry.AliasType.WEBAPP) {
                throw new HttpException(HttpServletResponse.SC_NOT_IMPLEMENTED,
                                "Method not implemented: " + request.getMethod());
            }

            /*
             * check for overwritten alias
             */
            final String alternate = entry.getOverwrite();
            if (alternate != null) {
                logger.debug("handle, overwritten alias: {}", alternate);
                final AliasEntry overwrittenEntry = aliases.get(alternate);
                if (overwrittenEntry != null) {
                    entry = overwrittenEntry;
                }
                logger.debug("handle, alias {}, entry {}", alternate, entry);
            }

            final Handler handler = entry.getHandler();
            logger.debug("got handler: {}", handler);
            if (handler != null) {
                handler.handle(target, baseRequest, request, response);
            }

        } catch (final Exception e) {
            if (entry != null && e.getCause() instanceof OnErrorException) {
                final String alternate = entry.getOnError();
                if (alternate != null) {
                    logger.debug("handle, onError alias: {}", alternate);
                    final AliasEntry overwrittenEntry = aliases.get(alternate);
                    if (overwrittenEntry != null) {
                        entry = overwrittenEntry;
                        logger.debug("handle, alias {}, entry {}", alternate,
                                        entry);
                        final Handler handler = entry.getHandler();
                        if (handler != null) {
                            try {
                                handler.handle(target, baseRequest, request, response);
                            } catch (final ServletException t) {
                                handleException(t, uri, response);
                            }
                        }
                    } else {
                        handleException(new HttpException(HttpServletResponse.SC_NOT_FOUND,
                                        "Not found : " + entry.getSpecificString()),
                                        uri, response);
                    }
                }
            } else {
                handleException(e, uri, response);
            }
        }

        logger.info("Finished");
    }

    public AliasEntry removeAlias(String name) throws InvocationTargetException
    {
        AliasEntry entry = aliases.remove(name);
        if (entry != null) {
            try {
                Handler handler = entry.getHandler();
                removeBean(handler);
                if (handler.isStarted()) {
                    handler.stop();
                }
            } catch (Exception e) {
                Throwables.propagateIfPossible(e);
                throw new InvocationTargetException(e, "Handler failed to stop.");
            }
        }
        return entry;
    }

    @Override
    protected void doStart() throws Exception
    {
        for (AliasEntry entry : aliases.values()) {
            entry.getHandler().setServer(getServer());
        }
        super.doStart();
    }

    public void addAlias(String name, AliasEntry entry) throws InvocationTargetException
    {
        Handler handler = entry.getHandler();
        addBean(handler, true);
        if (isStarted() && !handler.isStarted()) {
            try {
                handler.setServer(getServer());
                handler.start();
            } catch (Exception e) {
                Throwables.propagateIfPossible(e);
                throw new InvocationTargetException(e, "Handler failed to start.");
            }
        }
        aliases.put(name, entry);
    }

    public Map<String, AliasEntry> getAliases()
    {
        return Collections.unmodifiableMap(aliases);
    }

    public AliasEntry getAlias(String name)
    {
        return aliases.get(name);
    }
}
