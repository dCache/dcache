/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

/**
 *  Utility methods for URI objects.
 */
public class URIs
{
    private static final Map<String,Integer> TO_DEFAULT_PORT = ImmutableMap.<String,Integer>builder()
            .put("ftp", 21)
            .put("http", 80)
            .put("https", 443)
            .put("gsiftp", 2811)
            .put("gridftp", 2811)
            .put("ldap", 389)
            .put("ldaps", 636)
            .put("srm", 8443)
            .build();

    private URIs()
    {
        // It's a utility class!
    }

    /**
     * Obtain the TCP port number from the URI.  If the port number is
     * defined in the URI then that value is used.  If the URI contains no
     * port number but a default value is known for the schema, then that
     * default value is returned; otherwise, otherwise -1 is returned.
     */
    public static int portWithDefault(URI uri)
    {
        return portWithDefault(uri, null, -1);
    }

    /**
     * Obtain an optional port number based on supplied URI.  Use the
     * defined port number, if the URI defines one; otherwise use the default
     * port number for URI's schema, if one is known.  Otherwise return an
     * empty Optional.
     */
    public static Optional<Integer> optionalPortWithDefault(URI uri)
    {
        int port = portWithDefault(uri, null, -1);
        return port > -1 ? Optional.of(port) : Optional.<Integer>empty();
    }

    /**
     * Obtain a port number based on the supplied URI, using defaults as
     * a fall-back.  The supplied (schema, port) mapping is used
     * preferentially.  If the URI contains no port number and the schema has
     * no default then return -1.
     */
    public static int portWithDefault(URI uri, String defaultScheme, int defaultPort)
    {
        int port = uri.getPort();

        if (uri.getPort() == -1) {
            String scheme = uri.getScheme();

            if (scheme != null) {
                if (scheme.equals(defaultScheme)) {
                    port = defaultPort;
                } else {
                    Integer fromDefaults = TO_DEFAULT_PORT.get(scheme);

                    if (fromDefaults != null) {
                        port = fromDefaults;
                    }
                }
            }
        }

        return port;
    }

    private static boolean isDefaultPortNeeded(URI uri)
    {
        return uri.getPort() == -1 && uri.getHost() != null;
    }

    /**
     * Return a URI with a defined port number, if possible.  If the URI
     * has no port number then the argument-supplied (schema,port) mapping is
     * used if the scheme matches, otherwise default port numbers are used if a
     * default port is known for this scheme.
     */
    public static URI withDefaultPort(URI uri, String scheme, int port)
    {
        if (isDefaultPortNeeded(uri)) {
            int defaultPort = portWithDefault(uri, scheme, port);

            if (defaultPort != -1) {
                try {
                    return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(),
                            defaultPort, uri.getPath(), uri.getQuery(), uri.getFragment());
                } catch (URISyntaxException e) {
                    throw new RuntimeException("Failed to add default port: " + e.getMessage(), e);
                }
            }
        }

        return uri;
    }

    /**
     * Return a URI with a defined port number, if possible.  Default port
     * numbers are used are used if the URI has no port number and a default
     * port is known for this scheme.
     */
    public static URI withDefaultPort(URI uri)
    {
        return withDefaultPort(uri, null, -1);
    }

    /**
     * Create a new URI from the string representation, using default port
     * numbers if the string does not specify any.  The argument-supplied
     * (schema,port) mapping overrides any default mapping,
     */
    public static URI createWithDefaultPort(String uri, String scheme, int port) throws URISyntaxException
    {
        return withDefaultPort(new URI(uri), scheme, port);
    }

    /**
     * Create a new URI from the string representation, using default port
     * numbers if the string does not specify any.
     */
    public static URI createWithDefaultPort(String uri) throws URISyntaxException
    {
        return withDefaultPort(new URI(uri));
    }
}
