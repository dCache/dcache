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
package org.dcache.gplazma.oidc.jwt;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.client.HttpClient;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.ExtractResult;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.TokenProcessor;
import org.dcache.gplazma.oidc.UnableToProcess;
import org.dcache.gplazma.util.JsonWebToken;

import static org.dcache.gplazma.oidc.PropertiesUtils.asIntOrDefault;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * This class implements the offline verification of JWTs.
 */
public class OfflineJwtVerification implements TokenProcessor {

    private final Map<String, Issuer> issuersByEndpoint;

    public OfflineJwtVerification(Properties properties, HttpClient client,
            Set<IdentityProvider> providers) {
        this(properties, asIntOrDefault(properties, "gplazma.oidc.token-history", 0), client, providers);
    }

    private OfflineJwtVerification(Properties properties, int history, HttpClient client,
            Set<IdentityProvider> providers) {
        this(properties, providers.stream()
                .map(p -> new Issuer(client, p, history))
                .collect(Collectors.toList()));
    }

    @VisibleForTesting
    OfflineJwtVerification(Properties properties, Collection<Issuer> issuers) {
        issuersByEndpoint = issuers.stream().collect(Collectors.toMap(Issuer::getEndpoint, i -> i));
    }

    @Override
    public ExtractResult extract(String token) throws AuthenticationException, UnableToProcess {
        if (!JsonWebToken.isCompatibleFormat(token)) {
            throw new UnableToProcess("token not JWT");
        }

        try {
            var jwt = checkValid(new JsonWebToken(token));

            var issuer = issuerOf(jwt);

            return new ExtractResult(issuer.getIdentityProvider(), jwt.getPayloadMap());
        } catch (IOException e) {
            throw new UnableToProcess(e.getMessage());
        }
    }

    private JsonWebToken checkValid(JsonWebToken token) throws AuthenticationException {
        Instant now = Instant.now();

        Optional<Instant> exp = token.getPayloadInstant("exp");
        checkAuthentication(!exp.isPresent() || now.isBefore(exp.get()),
              "has expired");

        Optional<Instant> nbf = token.getPayloadInstant("nbf");
        checkAuthentication(!nbf.isPresent() || now.isAfter(nbf.get()),
              "is not yet valid");

        return token;
    }

    private Issuer issuerOf(JsonWebToken token) throws AuthenticationException {
        var issuerEndpoint = token.getPayloadString("iss")
              .orElseThrow(() -> new AuthenticationException("Missing 'iss' in JWT"));

        var issuer = issuersByEndpoint.get(issuerEndpoint);
        checkAuthentication(issuer != null, "Untrusted issuer %s", issuerEndpoint);

        issuer.checkIssued(token);

        return issuer;
    }
}
