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

import javax.servlet.http.HttpServletRequest;

/**
 * Class checking whether a request's User-agent is the OwnCloud Sync client.
 */
public class OwncloudClients
{

    private static final String OWNCLOUD_USERAGENT = "mirall";

    public static boolean isSyncClient(HttpServletRequest request)
    {
        String userAgent = request.getHeader(HttpHeader.USER_AGENT.toString());

        return userAgent != null && userAgent.contains(OWNCLOUD_USERAGENT);
    }
}
