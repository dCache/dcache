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
package org.dcache.restful.util;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

/**
 * Fills {@code dcache-view.oidc-authz-endpoint-list} from OIDC discovery when the admin
 * did not set it. Issuers are taken from {@code dcache-view.oidc-issuer-list} when that
 * is set, otherwise from {@code frontend.authn.oidc.issuer}.
 */
public class OidcDiscoveryConfigDecorator implements FactoryBean<Map<String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OidcDiscoveryConfigDecorator.class);

    static final String AUTHZ_ENDPOINT_LIST = "dcache-view.oidc-authz-endpoint-list";
    static final String ISSUER_LIST = "dcache-view.oidc-issuer-list";

    private Map<String, String> delegate;
    private String issuer;
    private OidcDiscovery oidcDiscovery;

    public void setDelegate(Map<String, String> delegate) {
        this.delegate = delegate;
    }

    public void setIssuer(String issuer) {
        this.issuer = issuer;
    }

    public void setOidcDiscovery(OidcDiscovery oidcDiscovery) {
        this.oidcDiscovery = oidcDiscovery;
    }

    @Override
    public Map<String, String> getObject() {
        return enrich(delegate);
    }

    Map<String, String> enrich(Map<String, String> data) {
        if (data == null) {
            return ImmutableMap.of();
        }
        if (OidcDiscovery.hasText(data.get(AUTHZ_ENDPOINT_LIST))) {
            return data;
        }
        List<String> issuers = issuersFrom(data);
        if (issuers.isEmpty()) {
            return data;
        }
        List<String> endpoints = new ArrayList<>();
        for (String iss : issuers) {
            try {
                endpoints.add(oidcDiscovery.authorizationEndpoint(URI.create(iss)));
            } catch (Exception e) {
                LOGGER.warn("Failed to discover authorization_endpoint for {}: {}", iss,
                      e.toString());
            }
        }
        if (endpoints.isEmpty()) {
            return data;
        }
        Map<String, String> copy = new LinkedHashMap<>(data);
        copy.put(AUTHZ_ENDPOINT_LIST, String.join(" ", endpoints));
        return ImmutableMap.copyOf(copy);
    }

    private List<String> issuersFrom(Map<String, String> data) {
        String listed = data.get(ISSUER_LIST);
        if (OidcDiscovery.hasText(listed)) {
            return Splitter.on(' ').omitEmptyStrings().trimResults().splitToList(listed);
        }
        if (OidcDiscovery.hasText(issuer)) {
            return List.of(issuer.trim());
        }
        return List.of();
    }

    @Override
    public Class<?> getObjectType() {
        return Map.class;
    }
}
