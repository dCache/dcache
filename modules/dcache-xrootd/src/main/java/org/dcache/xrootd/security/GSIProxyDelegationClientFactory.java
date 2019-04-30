/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd.security;

import java.util.Properties;

import org.dcache.xrootd.plugins.ProxyDelegationClientFactory;
import org.dcache.xrootd.plugins.authn.gsi.X509ProxyDelegationClient;

/**
 * Implements factory which creates the client specific to delegation
 * and caching of X509 credentials.
 */
public class GSIProxyDelegationClientFactory implements
                ProxyDelegationClientFactory<X509ProxyDelegationClient>
{
    private static final String PROTOCOL = "gsi";

    @Override
    public X509ProxyDelegationClient
        createClient(String name, Properties properties) {
        return PROTOCOL.equals(name) ?
                        new GSIProxyDelegationClient(properties) : null;
    }
}
