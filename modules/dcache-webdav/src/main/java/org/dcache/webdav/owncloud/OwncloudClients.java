/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.webdav.owncloud;

import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.time.Instant;
import java.util.Optional;

/**
 * Class checking whether a request's User-agent is the OwnCloud Sync client.
 */
public class OwncloudClients
{
    private static final Logger LOG = LoggerFactory.getLogger(OwncloudClients.class);
    private static final String OWNCLOUD_USERAGENT = "mirall";


    /**
     * Method checking whether a request is made by the OwnCloud Sync client.
     */
    public static boolean isSyncClient(HttpServletRequest request)
    {
        String userAgent = request.getHeader(HttpHeader.USER_AGENT.toString());

        return userAgent != null && userAgent.contains(OWNCLOUD_USERAGENT);
    }

    /**
     * Provide a file's mtime, as supplied by the OwnCloud Sync client.
     * @return Optionally the client stated mtime instance.
     */
    public static Optional<Instant> parseMTime(HttpServletRequest request)
    {
        /*
         * See https://github.com/owncloud/client/blob/v2.2.3/src/libsync/propagateupload.cpp#L498
         *
         * NB. lower-case 't' in "X-OC-Mtime"
         */
        String value = request.getHeader("X-OC-Mtime");

        try {
            if (value != null) {
                return Optional.of(Instant.ofEpochSecond(Long.parseLong(value)));
            }
        } catch (NumberFormatException e) {
            LOG.info("Invalid mtime header: {}", e.getMessage());
        }

        return Optional.empty();
    }

    /**
     * Update reply to indicate that the supplied mtime was accepted.
     */
    public static void addMTimeAccepted(HttpServletResponse response)
    {
        /*
         * See https://github.com/owncloud/client/blob/v2.2.3/src/libsync/propagateupload.cpp#L768
         *
         * NB. upper-case 'T' in "X-OC-MTime"
         */
        response.setHeader("X-OC-MTime", "accepted");
    }
}
