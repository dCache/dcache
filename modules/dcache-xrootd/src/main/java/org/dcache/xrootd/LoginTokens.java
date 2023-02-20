/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Splitter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to handle login token.
 */
public class LoginTokens
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoginTokens.class);

    private static final String JOIN_ELEMENT = "&";
    private static final char KEY_VALUE_SEPARATOR = '=';
    private static final String HOST_AND_PORT_KEY = "org.dcache.door";
    private static final char HOST_PORT_SEPERATOR = ':';

    private LoginTokens() {} // Prevent instantiation.

    public static String encodeToken(InetSocketAddress addr) {
        return HOST_AND_PORT_KEY + KEY_VALUE_SEPARATOR
                + addr.getHostString() + HOST_PORT_SEPERATOR + addr.getPort();
    }

    public static Optional<InetSocketAddress> decodeToken(String token) {
        try {
            /*checkArgument(token.startsWith(INITIAL_ELEMENT),
                    "Missing initial \"" + INITIAL_ELEMENT + "\"");*/

            Map<String,String> data = Splitter.on(JOIN_ELEMENT)
                    .withKeyValueSeparator(KEY_VALUE_SEPARATOR)
                    .split(token);
            String hostAndPort = data.get(HOST_AND_PORT_KEY);
            checkArgument(hostAndPort != null, "Missing \"" + HOST_AND_PORT_KEY + "\" key");

            int seperator = hostAndPort.indexOf(HOST_PORT_SEPERATOR);
            checkArgument(seperator > -1, "Missing '" + HOST_PORT_SEPERATOR + "' in "
                    + HOST_AND_PORT_KEY + " value");
            checkArgument(seperator > 0, "'" + HOST_PORT_SEPERATOR
                    + "' cannot be first character in " + HOST_AND_PORT_KEY);
            checkArgument(seperator < hostAndPort.length()-1, "'" + HOST_PORT_SEPERATOR
                    + "' cannot be last character in " + HOST_AND_PORT_KEY);

            String host = hostAndPort.substring(0, seperator);
            String port = hostAndPort.substring(seperator+1);

            InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName(host),
                    Integer.parseInt(port));
            return Optional.of(addr);
        } catch (UnknownHostException | IllegalArgumentException e) {
            LOGGER.debug("Bad kXR_login token \"{}\": {}", token, e.getMessage());
        }

        return Optional.empty();
    }
}
