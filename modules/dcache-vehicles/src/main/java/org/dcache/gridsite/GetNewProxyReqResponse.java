/*
 * dCache - http://www.dcache.org/
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
package org.dcache.gridsite;

import java.io.Serializable;

public class GetNewProxyReqResponse implements Serializable
{
    private static final long serialVersionUID = 9081810451100249770L;
    private final String proxyRequest;
    private final String delegationID;

    public GetNewProxyReqResponse(String proxyRequest, String delegationID)
    {
        this.proxyRequest = proxyRequest;
        this.delegationID = delegationID;
    }

    public String getProxyRequest()
    {
        return proxyRequest;
    }

    public String getDelegationID()
    {
        return delegationID;
    }
}
