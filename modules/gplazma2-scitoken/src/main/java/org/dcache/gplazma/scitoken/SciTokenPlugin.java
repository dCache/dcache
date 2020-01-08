/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.scitoken;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import diskCacheV111.util.FsPath;

import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.JwtJtiPrincipal;
import org.dcache.auth.JwtSubPrincipal;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.MultiTargetedRestriction;
import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.util.JsonWebToken;
import org.dcache.util.Args;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * An authentication plugin that supports SciToken bearer tokens.
 */
public class SciTokenPlugin implements GPlazmaAuthenticationPlugin
{
    private static final EnumSet<Activity> READ_ACTIVITY = EnumSet.of(Activity.LIST, Activity.READ_METADATA, Activity.DOWNLOAD);
    private static final EnumSet<Activity> WRITE_ACTIVITY = EnumSet.of(Activity.LIST, Activity.READ_METADATA, Activity.UPLOAD, Activity.MANAGE, Activity.DELETE, Activity.UPDATE_METADATA);

    private final HttpClient client;
    private final Map<String,Issuer> issuersByEndpoint;
    private final Set<String> audienceTargets;

    private int tokenHistory = 0;

    public SciTokenPlugin(Properties properties)
    {
        this(properties, HttpClients.createDefault());
    }

    public SciTokenPlugin(Properties properties, HttpClient client)
    {
        this.client = requireNonNull(client);
        issuersByEndpoint = properties.stringPropertyNames().stream()
                .filter(this::isIssuer)
                .map(k -> buildIssuer(k, properties.getProperty(k)))
                .collect(Collectors.toMap(i -> i.getEndpoint(), i -> i));

        String history = properties.getProperty("gplazma.scitoken.token-history");
        tokenHistory = Integer.parseInt(history);

        String targets = properties.getProperty("gplazma.scitoken.audience-targets");
        audienceTargets = ImmutableSet.copyOf(Splitter.on(' ').trimResults().split(targets));
    }

    private boolean isIssuer(Object key)
    {
        return key instanceof String && ((String)key).startsWith("gplazma.scitoken.issuer!");
    }

    private Issuer buildIssuer(Object key, Object value)
    {
        String id = String.valueOf(key).substring(24);
        checkArgument(!id.isEmpty(), "Bad gplazma.scitoken.issuer: missing id");

        Args args = new Args(String.valueOf(value));
        checkArgument(args.argc() > 0, "Missing Issuer URL");

        String endpoint = args.argv(0);
        FsPath prefix = FsPath.create(args.argv(1));
        args.shift(2);

        int issuerTokenHistory = args.hasOption("tokenHistory")
                ? args.getIntOption("tokenHistory") : tokenHistory;

        Set<Principal> principals = Subjects.principalsFromArgs(args.getArguments());
        return new Issuer(client, id, endpoint, prefix, principals, issuerTokenHistory);
    }

    @Override
    public void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials,
            Set<Principal> identifiedPrincipals, Set<Restriction> restrictions)
            throws AuthenticationException
    {
        List<String> tokens = privateCredentials.stream()
                .filter(BearerTokenCredential.class::isInstance)
                .map(BearerTokenCredential.class::cast)
                .map(BearerTokenCredential::getToken)
                .filter(JsonWebToken::isCompatibleFormat)
                .collect(Collectors.toList());

        checkAuthentication(!tokens.isEmpty(), "no JWT bearer token");
        checkAuthentication(tokens.size() == 1, "multiple JWT bearer tokens");

        try {
            JsonWebToken token = checkValid(new JsonWebToken(tokens.get(0)));
            Issuer issuer = issuerOf(token);

            Collection<Principal> principals = new ArrayList<>();

            Optional<String> sub = token.getPayloadString("sub");
            sub.map(s -> new JwtSubPrincipal(issuer.getId(), s))
                .ifPresent(principals::add);

            Optional<String> jti = token.getPayloadString("jti");
            jti.map(s -> new JwtJtiPrincipal(issuer.getId(), s))
                .ifPresent(principals::add);

            checkAuthentication(sub.isPresent() || jti.isPresent(), "missing sub and jti claims");

            principals.addAll(issuer.getPrincipals());

            String scope = token.getPayloadString("scope")
                    .orElseThrow(() -> new AuthenticationException("missing scope claim"));
            List<SciTokenScope> scopes = SciTokenScope.parseScope(scope);
            checkAuthentication(!scopes.isEmpty(), "not a SciToken: found no SciToken scope terms.");

            identifiedPrincipals.addAll(principals);
            Restriction r = buildRestriction(issuer.getPrefix(), scopes);
            restrictions.add(r);
        } catch (IOException e) {
            throw new AuthenticationException(e.getMessage());
        }
    }

    private JsonWebToken checkValid(JsonWebToken token) throws AuthenticationException
    {
        Instant now = Instant.now();

        Optional<Instant> exp = token.getPayloadInstant("exp");
        checkAuthentication(!exp.isPresent() || now.isBefore(exp.get()),
                "has expired");

        Optional<Instant> nbf = token.getPayloadInstant("nbf");
        checkAuthentication(!nbf.isPresent() || now.isAfter(nbf.get()),
                "is not yet valid");

        // REVISIT obtain door IP address and DNS lookup URL to see if it matches
        List<String> aud = token.getPayloadStringOrArray("aud");
        checkAuthentication(aud.isEmpty() || audienceTargets.stream().anyMatch(aud::contains),
                "intended for %s", aud);

        return token;
    }

    private Issuer issuerOf(JsonWebToken token) throws AuthenticationException
    {
        String issuerEndpoint = token.getPayloadString("iss")
                .orElseThrow(() -> new AuthenticationException("Missing 'iss' in JWT"));

        Issuer issuer = issuersByEndpoint.get(issuerEndpoint);
        checkAuthentication(issuer != null, "Untrusted issuer %s", issuerEndpoint);

        issuer.checkIssued(token);

        return issuer;
    }

    private Restriction buildRestriction(FsPath prefix, List<SciTokenScope> scopes)
    {
        List<Authorisation> authorisations = new ArrayList<>();

        for (SciTokenScope scope : scopes) {
            FsPath path = prefix.resolve(scope.getPath().substring(1));
            switch (scope.getOperation()) {
            case READ:
                authorisations.add(new Authorisation(READ_ACTIVITY, path));
                break;

            case WRITE:
                authorisations.add(new Authorisation(WRITE_ACTIVITY, path));
                break;
            }
        }

        return new MultiTargetedRestriction(authorisations);
    }
}
