/**
 * Copyright (C) 2011-2021 dCache.org <support@dcache.org>
 * <p>
 * This file is part of xrootd4j.
 * <p>
 * xrootd4j is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * <p>
 * xrootd4j is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License along with xrootd4j.  If
 * not, see http://www.gnu.org/licenses/.
 */
package org.dcache.xrootd.plugins.authn;

import java.util.Properties;
import org.dcache.xrootd.plugins.AuthenticationHandler;
import org.dcache.xrootd.plugins.ProxyDelegationClient;
import org.dcache.xrootd.plugins.authn.ztn.AbstractZTNAuthenticationFactory;

/**
 * Authentication factory that returns ztn authentication handlers.
 */
public class BearerTokenZTNAuthnFactory extends AbstractZTNAuthenticationFactory {

    public BearerTokenZTNAuthnFactory(Properties properties)
          throws ClassNotFoundException {
        super(properties);
    }

    @Override
    public AuthenticationHandler createHandler(ProxyDelegationClient client) {
        BearerTokenZTNAuthnHandler handler = new BearerTokenZTNAuthnHandler();
        handler.setMaxTokenSize(maxTokenSize);
        handler.setTokenUsageFlags(tokenUsageFlags);
        handler.setAlternateTokenLocations(alternateTokenLocations);
        return handler;
    }
}
