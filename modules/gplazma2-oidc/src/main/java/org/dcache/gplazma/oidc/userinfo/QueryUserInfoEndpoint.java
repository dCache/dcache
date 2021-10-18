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
package org.dcache.gplazma.oidc.userinfo;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.ExtractResult;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.TokenProcessor;
import org.dcache.gplazma.oidc.helpers.JsonHttpClient;
import org.dcache.gplazma.util.JsonWebToken;
import org.dcache.util.BoundedCachedExecutor;
import org.dcache.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.dcache.gplazma.oidc.PropertiesUtils.asInt;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * This TokenProcessor queries the user-info endpoint to learn the claims about the user. The
 * results are cached for a period, to ensure we do not hammer the server and (potentially) to
 * improve latency.
 */
public class QueryUserInfoEndpoint implements TokenProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(QueryUserInfoEndpoint.class);

    private final static String ACCESS_TOKEN_CACHE_SIZE = "gplazma.oidc.access-token-cache.size";
    private final static String ACCESS_TOKEN_CACHE_REFRESH = "gplazma.oidc.access-token-cache.refresh";
    private final static String ACCESS_TOKEN_CACHE_REFRESH_UNIT = "gplazma.oidc.access-token-cache.refresh.unit";
    private final static String ACCESS_TOKEN_CACHE_EXPIRE = "gplazma.oidc.access-token-cache.expire";
    private final static String ACCESS_TOKEN_CACHE_EXPIRE_UNIT = "gplazma.oidc.access-token-cache.expire.unit";
    private final static String CONCURRENCY = "gplazma.oidc.concurrent-requests";
    private final static String DISCOVERY_CACHE_REFRESH = "gplazma.oidc.discovery-cache";
    private final static String DISCOVERY_CACHE_REFRESH_UNIT = "gplazma.oidc.discovery-cache.unit";
    private final static String HTTP_SLOW_LOOKUP = "gplazma.oidc.http.slow-threshold";
    private final static String HTTP_SLOW_LOOKUP_UNIT = "gplazma.oidc.http.slow-threshold.unit";

    private final JsonHttpClient jsonHttpClient;
    private final ExecutorService executor;
    private final LoadingCache<IdentityProvider, JsonNode> discoveryCache;
    private final LoadingCache<String, List<LookupResult>> userInfoCache;
    private final Map<URI, IdentityProvider> providersByIssuer;
    private final Duration slowLookupThreshold;

    public QueryUserInfoEndpoint(Properties properties, JsonHttpClient client,
            Set<IdentityProvider> providers) {
        jsonHttpClient = requireNonNull(client);

        int concurrency = asInt(properties, CONCURRENCY);
        executor = new BoundedCachedExecutor(concurrency);

        providersByIssuer = providers.stream()
              .collect(toMap(IdentityProvider::getIssuerEndpoint, p -> p));

        discoveryCache = createDiscoveryCache(asInt(properties, DISCOVERY_CACHE_REFRESH),
              TimeUnit.valueOf(properties.getProperty(DISCOVERY_CACHE_REFRESH_UNIT)));
        slowLookupThreshold = Duration.of(asInt(properties, HTTP_SLOW_LOOKUP),
              ChronoUnit.valueOf(properties.getProperty(HTTP_SLOW_LOOKUP_UNIT)));
        userInfoCache = createUserInfoCache(asInt(properties, ACCESS_TOKEN_CACHE_SIZE),
              asInt(properties, ACCESS_TOKEN_CACHE_REFRESH),
              TimeUnit.valueOf(properties.getProperty(ACCESS_TOKEN_CACHE_REFRESH_UNIT)),
              asInt(properties, ACCESS_TOKEN_CACHE_EXPIRE),
              TimeUnit.valueOf(properties.getProperty(ACCESS_TOKEN_CACHE_EXPIRE_UNIT)));
    }

    @Override
    public void shutdown() {
        executor.shutdownNow();
    }

    @Override
    public ExtractResult extract(String token) throws AuthenticationException {
        Stopwatch userinfoLookupTiming = Stopwatch.createStarted();

        List<LookupResult> allResults;

        try {
            allResults = userInfoCache.get(token);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.throwIfInstanceOf(cause, AuthenticationException.class);
            Throwables.throwIfUnchecked(cause);
            if (cause instanceof InterruptedException) {
                throw new AuthenticationException("Shutting down");
            }
            throw new RuntimeException("Unexpected exception", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Doing user-info lookup against {} OPs took {}",
                  allResults.size(),
                  TimeUtils.describe(userinfoLookupTiming.elapsed()).orElse("no time"));
        }

        List<LookupResult> successfulResults = allResults.stream().filter(LookupResult::isSuccess)
              .collect(Collectors.toList());

        if (successfulResults.isEmpty()) {
            if (allResults.size() == 1) {
                LookupResult result = allResults.get(0);
                throw new AuthenticationException(
                      "OpenId Validation failed for " + result.getIdentityProvider().getName()
                            + ": " + result.getError());
            } else {
                String randomId = randomId();
                String errors = allResults.stream()
                      .map(r -> "[" + r.getIdentityProvider().getName() + ": " + r.getError() + "]")
                      .collect(Collectors.joining(", "));
                LOG.warn("OpenId Validation Failure ({}): {}", randomId, errors);
                throw new AuthenticationException(
                      "OpenId Validation Failed check [log entry #" + randomId + "]");
            }
        }

        if (successfulResults.size() > 1) {
            String names = successfulResults.stream()
                  .map(LookupResult::getIdentityProvider)
                  .map(IdentityProvider::getName)
                  .collect(Collectors.joining(", "));
            LOG.warn("Multiple OpenID-Connect endpoints accepted access token: {}", names);
            throw new AuthenticationException("Multiple OPs accepted token.");
        }

        var result = successfulResults.get(0);
        return new ExtractResult(result.getIdentityProvider(), result.getClaims());
    }

    private LoadingCache<IdentityProvider, JsonNode> createDiscoveryCache(int refresh,
          TimeUnit refreshUnits) {
        return CacheBuilder.newBuilder()
              .maximumSize(100)
              .refreshAfterWrite(refresh, refreshUnits)
              .build(
                    new CacheLoader<IdentityProvider, JsonNode>() {
                        @Override
                        public JsonNode load(IdentityProvider provider)
                              throws OidcException, IOException {
                            LOG.debug("Fetching discoveryDoc for {}", provider.getName());
                            URI configuration = provider.getConfigurationEndpoint();
                            JsonNode discoveryDoc = jsonHttpClient.doGet(configuration);
                            if (discoveryDoc != null && discoveryDoc.has("userinfo_endpoint")) {
                                return discoveryDoc;
                            } else {
                                throw new OidcException(provider.getName(),
                                      "Discovery Document at " + discoveryDoc +
                                            " does not contain userinfo endpoint url");
                            }
                        }

                        @Override
                        public ListenableFuture<JsonNode> reload(final IdentityProvider provider,
                              JsonNode value) {
                            ListenableFutureTask<JsonNode> task = ListenableFutureTask.create(
                                  () -> load(provider));
                            executor.execute(task);
                            return task;
                        }
                    }
              );
    }

    private LoadingCache<String, List<LookupResult>> createUserInfoCache(int size,
          int refresh, TimeUnit refreshUnits, int expire, TimeUnit expireUnits) {
        return CacheBuilder.newBuilder()
              .maximumSize(size)
              .refreshAfterWrite(refresh, refreshUnits)
              .expireAfterWrite(expire, expireUnits)
              .build(new CacheLoader<String, List<LookupResult>>() {
                  private ListenableFuture<List<LookupResult>> asyncFetch(String token)
                        throws AuthenticationException {
                      List<ListenableFuture<LookupResult>> futures =
                            new ArrayList<>();

                      for (IdentityProvider ip : identityProviders(token)) {
                          if (LOG.isDebugEnabled()) {
                              LOG.debug("Scheduling lookup against {} of token {}",
                                    ip.getName(), describe(token, 20));
                          }

                          ListenableFutureTask<LookupResult> lookupTask =
                                ListenableFutureTask.create(() -> queryUserInfo(ip, token));
                          executor.execute(lookupTask);
                          futures.add(lookupTask);
                      }

                      return Futures.allAsList(futures);
                  }

                  @Override
                  public List<LookupResult> load(String token)
                        throws InterruptedException, AuthenticationException {
                      if (LOG.isDebugEnabled()) {
                          LOG.debug("User-info cache miss for token {}",
                                describe(token, 20));
                      }

                      try {
                          return asyncFetch(token).get();
                      } catch (ExecutionException e) {
                          Throwable cause = e.getCause();
                          Throwables.throwIfInstanceOf(cause, AuthenticationException.class);
                          Throwables.throwIfUnchecked(cause);
                          throw new RuntimeException("Unexpected exception", e);
                      }
                  }

                  @Override
                  public ListenableFuture<List<LookupResult>> reload(String token,
                        List<LookupResult> results) throws AuthenticationException {
                      if (LOG.isDebugEnabled()) {
                          LOG.debug("Refreshing user-info for token {}", describe(token, 20));
                      }
                      return asyncFetch(token);
                  }
              });
    }

    private Collection<IdentityProvider> identityProviders(String token)
          throws AuthenticationException {
        if (JsonWebToken.isCompatibleFormat(token)) {
            try {
                JsonWebToken jwt = new JsonWebToken(token);
                Optional<String> iss = jwt.getPayloadString("iss");
                if (iss.isPresent()) {
                    try {
                        URI issuer = new URI(iss.get());
                        IdentityProvider ip = providersByIssuer.get(issuer);
                        checkAuthentication(ip != null, "JWT with unknown \"iss\" claim");
                        LOG.debug("Discovered token is JWT issued by {}", ip.getName());
                        return Collections.singleton(ip);
                    } catch (URISyntaxException e) {
                        LOG.debug("Bad \"iss\" claim \"{}\": {}", iss.get(),
                              e.toString());
                        throw new AuthenticationException("Bad \"iss\" claim in JWT");
                    }
                }
            } catch (IOException e) {
                LOG.debug("Failed to parse JWT: {}", e.toString());
                throw new AuthenticationException("Bad JWT");
            }
        }

        return providersByIssuer.values();
    }

    private LookupResult queryUserInfo(IdentityProvider ip, String token) {
        Stopwatch userinfoLookupTiming = null;

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Starting querying {} about token {}", ip.getName(), describe(token, 20));
            }

            JsonNode discoveryDoc = discoveryCache.get(ip);
            userinfoLookupTiming = Stopwatch.createStarted();
            String userInfoEndPoint = extractUserInfoEndPoint(discoveryDoc);
            Map<String,JsonNode> claims = claimsFromUserInfoEndpoint(token,
                  userInfoEndPoint);
            return LookupResult.success(ip, claims);
        } catch (OidcException oe) {
            return LookupResult.error(ip, oe.getMessage());
        } catch (ExecutionException e) {
            return LookupResult.error(ip, "(\"" + ip.getName() + "\", " + e.getMessage() + ")");
        } finally {
            if (userinfoLookupTiming != null) {
                userinfoLookupTiming.stop();

                if (userinfoLookupTiming.elapsed().compareTo(slowLookupThreshold) > 0) {
                    LOG.warn("OpenID-Connect user-info endpoint {} took {} to return",
                          ip.getName(),
                          TimeUtils.describe(userinfoLookupTiming.elapsed()).orElse("no time"));
                }
            }
        }
    }

    private String randomId() {
        byte[] rawId = new byte[6]; // a Base64 char represents 6 bits; 4 chars represent 3 bytes.
        ThreadLocalRandom.current().nextBytes(rawId);
        return Base64.getEncoder().withoutPadding().encodeToString(rawId);
    }

    private Map<String,JsonNode> claimsFromUserInfoEndpoint(String token, String infoUrl)
            throws OidcException {
        try {
            JsonNode userInfo = getUserInfo(infoUrl, token);
            if (userInfo == null || !userInfo.has("sub")) {
                throw new OidcException("No OpendId \"sub\"");
            }
            if (!userInfo.isObject()) {
                throw new OidcException("User-info endpoint returned a non-JSON object");
            }
            return Streams.stream(userInfo.fields())
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        } catch (IllegalArgumentException iae) {
            throw new OidcException("Error parsing UserInfo: " + iae.getMessage());
        } catch (IOException e) {
            throw new OidcException("Failed to fetch UserInfo: " + e.getMessage());
        }
    }


    private String extractUserInfoEndPoint(JsonNode discoveryDoc) {
        if (discoveryDoc.has("userinfo_endpoint")) {
            return discoveryDoc.get("userinfo_endpoint").asText();
        } else {
            return null;
        }
    }

    private JsonNode getUserInfo(String url, String token) throws OidcException, IOException {
        JsonNode userInfo = jsonHttpClient.doGetWithToken(url, token);
        if (userInfo == null) {
            throw new OidcException("Querying the user-info endpoint failed");
        }
        if (userInfo.has("error")) {
            String error = userInfo.get("error").asText();
            String errorDescription = userInfo.get("error_description").asText();
            throw new OidcException("User-info endpoint replied: [" + error + ", " + errorDescription + " ]");
        } else {
            return userInfo;
        }
    }

    private static String describe(String id, int limit) {
        if (id.length() < limit) {
            return id;
        } else {
            int length = (limit - 3) / 2;
            return id.substring(0, length) + "..." + id.substring(id.length() - length);
        }
    }
}
