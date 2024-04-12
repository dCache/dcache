/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2024 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.alise;

import com.google.common.annotations.VisibleForTesting;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Splitter;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.dcache.auth.OAuthProviderPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.util.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.Objects.requireNonNull;

/**
 * A mapping plugin that makes use of the Account LInking SErvice: ALISE.
 * <p>
 * ALISE allows a user to link their federated identity (an OIDC issuer + 'sub'
 * claim) and their site-local user identity (expressed as a username).  In
 * addition, ALISE returns other information, such as the user's display name.
 * <p>
 * ALISE works by allowing external services (such as dCache) to query its
 * internal database of mapped identity with an issuer and a sub-claim value.
 * If the query is successful then information about that user is returned,
 * otherwise an error is returned.  These queries require an API KEY.
 * <p>
 * For further details, see the project's
 * <a href="https://github.com/m-team-kit/alise/tree/master/alise">GitHub page</a>
 * <p>
 * TODO: ALISE provides a list of issuers it accepts.  This list is available
 * via the ${ALISE}/alise/supported_issuers endpoint.  A future version of this
 * plugin could maintain a (dCache-internal) cached copy of this list and use
 * it to avoid sending ALISE tokens that it does not support.
 */
public class AlisePlugin implements GPlazmaMappingPlugin {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlisePlugin.class);

    private final List<String> mappableIssuers;
    private final LookupAgent lookupAgent;

    @Nonnull
    private static String getRequiredProperty(Properties config, String key) {
        var value = config.getProperty(key);
        checkArgument(value != null, "Config property \"%s\" is missing.", key);
        return value;
    }

    private static LookupAgent buildLookupAgent(Properties config)
            throws URISyntaxException {
        var configEndpoint = getRequiredProperty(config,"gplazma.alise.endpoint");
        var endpoint = new URI(configEndpoint);
        var apikey = getRequiredProperty(config,"gplazma.alise.apikey");
        var target = getRequiredProperty(config, "gplazma.alise.target");
        var timeout = getRequiredProperty(config, "gplazma.alise.timeout");
        LookupAgent alise = new AliseLookupAgent(endpoint, target, apikey, timeout);

        LookupAgent cache = new CachingLookupAgent(alise);

        return cache;
    }

    public AlisePlugin(Properties config) throws URISyntaxException {
        this(config, buildLookupAgent(config));
    }

    @VisibleForTesting
    AlisePlugin(Properties config, LookupAgent agent) {
        var issuerList = getRequiredProperty(config, "gplazma.alise.issuers");
        mappableIssuers = Splitter.on(' ').omitEmptyStrings().splitToList(issuerList);
        lookupAgent = requireNonNull(agent);
    }

    private Optional<Identity> toIdentity(OidcSubjectPrincipal principal,
            Map<String,URI> issuers) {
        String issuerAlias = principal.getOP();
        URI issuer = issuers.get(issuerAlias);
        if (issuer == null) {
            // This is probably a bug, skip this identity.
            LOGGER.warn("{} has no corresponding OAuthProviderPrincipal",
                    principal);
            return Optional.empty();
        }

        if (!mappableIssuers.isEmpty()
                && !mappableIssuers.contains(issuerAlias)
                && !mappableIssuers.contains(issuer.toASCIIString())) {
            LOGGER.debug("{} rejected because not issued by allowed OP",
                    principal);
            return Optional.empty();
        }

        return Optional.of(new Identity(issuer, principal.getSubClaim()));
    }

    private Result<List<Identity>,String> buildIdentityList(Collection<Principal> principals)
            throws AuthenticationException {
        var allSubs = principals.stream()
                .filter(OidcSubjectPrincipal.class::isInstance)
                .map(OidcSubjectPrincipal.class::cast)
                .collect(toList());

        if (allSubs.isEmpty()) {
            return Result.failure("No 'sub' claims");
        }

        Map<String,URI> allIssuers = principals.stream()
                .filter(OAuthProviderPrincipal.class::isInstance)
                .map(OAuthProviderPrincipal.class::cast)
                .collect(toMap(OAuthProviderPrincipal::getName,
                        OAuthProviderPrincipal::getIssuer));

        var identities = allSubs.stream()
                .map(s -> toIdentity(s, allIssuers))
                .flatMap(Optional::stream)
                .collect(toList());

        return identities.isEmpty()
                ? Result.failure("No mappable 'sub' claim")
                : Result.success(identities);
    }

    private String buildFailureMessage(List<Result<Collection<Principal>,String>> results) {
        return results.stream()
                .filter(Result::isFailure)
                .map(Result::getFailure)
                .map(Optional::orElseThrow)
                .collect(Collectors.joining(", "));
    }

    @Override
    public void map(Set<Principal> principals) throws AuthenticationException {

        var identities = buildIdentityList(principals);

        var lookups = identities.orElseThrow(AuthenticationException::new).stream()
                .map(lookupAgent::lookup)
                .collect(toList());

        if (lookups.stream().allMatch(Result::isFailure)) {
            throw new AuthenticationException("Lookup failed: "
                    + buildFailureMessage(lookups));
        }

        if (lookups.stream().anyMatch(Result::isFailure) && LOGGER.isInfoEnabled()) {
            LOGGER.info("Mapping succeeded with some failures: {}",
                    buildFailureMessage(lookups));
        }

        lookups.stream()
                .map(Result::getSuccess)
                .filter(Optional::isPresent)
                .flatMap(o -> o.orElseThrow().stream())
                .forEach(principals::add);
    }

    @Override
    public void stop() throws Exception {
        lookupAgent.shutdown();
    }
}
