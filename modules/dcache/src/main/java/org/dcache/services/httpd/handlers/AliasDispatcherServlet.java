/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.services.httpd.handlers;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import dmg.util.HttpBasicAuthenticationException;
import dmg.util.HttpException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.dcache.services.httpd.exceptions.OnErrorException;
import org.dcache.services.httpd.util.AliasEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for parsing the request to find the correct alias and passing the request to the
 * alias handler. Turns failures of alias handlers into HTTP error responses.
 *
 * @author arossi
 */
public class AliasDispatcherServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER
          = LoggerFactory.getLogger(AliasDispatcherServlet.class);
    private static final Splitter PATH_SPLITTER
          = Splitter.on('/').omitEmptyStrings();

    private final transient Map<String, AliasEntry> aliases = new ConcurrentHashMap<>();

    private static String extractAlias(String requestURI) {
        return Iterables.getFirst(PATH_SPLITTER.split(requestURI), "<home>");
    }

    private static void sendError(HttpException exception, String uri,
          HttpServletResponse response) throws IOException {
        LOGGER.warn("Problem with {}: {}", uri, exception.getMessage());
        if (response.isCommitted()) {
            return;
        }
        if (exception instanceof HttpBasicAuthenticationException) {
            final String realm
                  = ((HttpBasicAuthenticationException) exception).getRealm();
            response.setHeader("WWW-Authenticate", "Basic realm=\""
                  + realm + "\"");
        }
        response.sendError(exception.getErrorCode(), exception.getMessage());
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        String uri = request.getRequestURI();
        AliasEntry entry = null;

        try {
            String alias = extractAlias(uri);
            LOGGER.debug("handle {}, {}", uri, alias);

            entry = aliases.get(alias);
            if (entry == null) {
                entry = aliases.get("<default>");
            }
            if (entry == null) {
                throw new HttpException(HttpServletResponse.SC_NOT_FOUND,
                      "Alias not found : " + alias);
            }
            LOGGER.debug("alias: {}, entry {}", alias, entry);

            /*
             * Only GET and HEAD are served; the container drops the body of a HEAD response.
             * Exclusion of POST is absolute.
             */
            String method = request.getMethod();
            if (!method.equals("GET") && !method.equals("HEAD")) {
                response.setHeader("Allow", "GET, HEAD");
                throw new HttpException(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                      "Method not allowed: " + method);
            }

            /*
             * check for overwritten alias
             */
            String alternate = entry.getOverwrite();
            if (alternate != null) {
                LOGGER.debug("handle, overwritten alias: {}", alternate);
                AliasEntry overwrittenEntry = aliases.get(alternate);
                if (overwrittenEntry != null) {
                    entry = overwrittenEntry;
                }
                LOGGER.debug("handle, alias {}, entry {}", alternate, entry);
            }

            entry.getHandler().handle(request, response);
        } catch (OnErrorException e) {
            handleOnError(entry, uri, request, response);
        } catch (HttpException e) {
            sendError(e, uri, response);
        } catch (RuntimeException e) {
            LOGGER.warn("Bug found, please report it", e);
            sendError(new HttpException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                  "Internal problem processing request"), uri, response);
        }
    }

    /**
     * Answers with the alias given by the {@code -onError} option of {@code entry}.
     */
    private void handleOnError(AliasEntry entry, String uri, HttpServletRequest request,
          HttpServletResponse response) throws IOException {
        String alternate = entry.getOnError();
        AliasEntry onErrorEntry = alternate == null ? null : aliases.get(alternate);
        if (onErrorEntry == null) {
            sendError(new HttpException(HttpServletResponse.SC_NOT_FOUND,
                  "Not found : " + entry.getSpecificString()), uri, response);
            return;
        }
        LOGGER.debug("handle, alias {}, entry {}", alternate, onErrorEntry);
        try {
            onErrorEntry.getHandler().handle(request, response);
        } catch (HttpException e) {
            sendError(e, uri, response);
        } catch (OnErrorException e) {
            sendError(new HttpException(HttpServletResponse.SC_NOT_FOUND,
                  "Not found : " + onErrorEntry.getSpecificString()), uri, response);
        }
    }

    public AliasEntry removeAlias(String name) {
        return aliases.remove(name);
    }

    public void addAlias(String name, AliasEntry entry) {
        aliases.put(name, entry);
    }

    public Map<String, AliasEntry> getAliases() {
        return Collections.unmodifiableMap(aliases);
    }

    public AliasEntry getAlias(String name) {
        return aliases.get(name);
    }
}
