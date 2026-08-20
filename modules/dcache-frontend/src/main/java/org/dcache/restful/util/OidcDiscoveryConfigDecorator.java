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
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;
import org.dcache.restful.resources.auth.OidcCodeFlowCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

/**
 * Fills {@code dcache-view.oidc-authz-endpoint-list} from OIDC discovery when the admin
 * did not set it. Issuers are taken from {@code dcache-view.oidc-issuer-list} when that
 * is set, otherwise from {@code frontend.authn.oidc.issuer}.
 * <p>
 * Also publishes {@code dcache-view.oidc-callback-path}, read via reflection off
 * {@link OidcCodeFlowCallback}'s own {@code @Path} annotations, so the redirect_uri
 * built by dCache View's login page never has to hard-code that path itself.
 */
public class OidcDiscoveryConfigDecorator implements FactoryBean<Map<String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OidcDiscoveryConfigDecorator.class);

    static final String AUTHZ_ENDPOINT_LIST = "dcache-view.oidc-authz-endpoint-list";
    static final String ISSUER_LIST = "dcache-view.oidc-issuer-list";
    static final String CALLBACK_PATH = "dcache-view.oidc-callback-path";

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
        Map<String, String> result = new LinkedHashMap<>(data == null ? Map.of() : data);

        if (!OidcDiscovery.hasText(result.get(CALLBACK_PATH))) {
            result.put(CALLBACK_PATH, discoverCallbackPath());
        }

        if (!OidcDiscovery.hasText(result.get(AUTHZ_ENDPOINT_LIST))) {
            List<String> issuers = issuersFrom(result);
            if (!issuers.isEmpty()) {
                List<String> endpoints = new ArrayList<>();
                for (String iss : issuers) {
                    try {
                        endpoints.add(oidcDiscovery.authorizationEndpoint(URI.create(iss)));
                    } catch (Exception e) {
                        LOGGER.warn("Failed to discover authorization_endpoint for {}: {}", iss,
                              e.toString());
                    }
                }
                if (!endpoints.isEmpty()) {
                    result.put(AUTHZ_ENDPOINT_LIST, String.join(" ", endpoints));
                }
            }
        }

        return Map.copyOf(result);
    }

    /**
     * Reads {@code @Path("/auth")} off {@link OidcCodeFlowCallback} and {@code @Path("/callback")}
     * off its {@code callback} method, so the path dCache View's login page redirects back to is
     * always the resource's real route rather than a separately typed-out literal.
     */
    private static String discoverCallbackPath() {
        try {
            Path classPath = OidcCodeFlowCallback.class.getAnnotation(Path.class);
            Method callback = OidcCodeFlowCallback.class.getMethod("callback",
                  String.class, HttpServletRequest.class, HttpServletResponse.class);
            Path methodPath = callback.getAnnotation(Path.class);
            return (classPath.value() + methodPath.value()).replaceFirst("^/+", "");
        } catch (NoSuchMethodException e) {
            throw new AssertionError(
                  "OidcCodeFlowCallback.callback signature no longer matches this reflection call",
                  e);
        }
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
