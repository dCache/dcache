package org.dcache.gplazma.oidc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InternetDomainName;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.LoA;
import org.dcache.auth.LoAPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.exceptions.OidcException;
import org.dcache.gplazma.oidc.helpers.JsonHttpClient;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.util.JsonWebToken;
import org.dcache.util.BoundedCachedExecutor;
import org.dcache.util.TimeUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toMap;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

public class OidcAuthPlugin implements GPlazmaAuthenticationPlugin
{
    private final static Logger LOG = LoggerFactory.getLogger(OidcAuthPlugin.class);
    private final static String OIDC_HOSTNAMES = "gplazma.oidc.hostnames";
    private final static String OIDC_PROVIDER_PREFIX = "gplazma.oidc.provider!";

    private final static String HTTP_CONCURRENT_ACCESS = "gplazma.oidc.http.total-concurrent-requests";
    private final static String HTTP_PER_ROUTE_CONCURRENT_ACCESS = "gplazma.oidc.http.per-route-concurrent-requests";
    private final static String HTTP_SLOW_LOOKUP = "gplazma.oidc.http.slow-threshold";
    private final static String HTTP_SLOW_LOOKUP_UNIT = "gplazma.oidc.http.slow-threshold.unit";
    private final static String HTTP_TIMEOUT = "gplazma.oidc.http.timeout";
    private final static String HTTP_TIMEOUT_UNIT = "gplazma.oidc.http.timeout.unit";
    private final static String CONCURRENCY = "gplazma.oidc.concurrent-requests";
    private final static String DISCOVERY_CACHE_REFRESH = "gplazma.oidc.discovery-cache";
    private final static String DISCOVERY_CACHE_REFRESH_UNIT = "gplazma.oidc.discovery-cache.unit";
    private final static String ACCESS_TOKEN_CACHE_SIZE = "gplazma.oidc.access-token-cache.size";
    private final static String ACCESS_TOKEN_CACHE_REFRESH = "gplazma.oidc.access-token-cache.refresh";
    private final static String ACCESS_TOKEN_CACHE_REFRESH_UNIT = "gplazma.oidc.access-token-cache.refresh.unit";
    private final static String ACCESS_TOKEN_CACHE_EXPIRE = "gplazma.oidc.access-token-cache.expire";
    private final static String ACCESS_TOKEN_CACHE_EXPIRE_UNIT = "gplazma.oidc.access-token-cache.expire.unit";

    /**
     * A mapping from "eduperson_assurance" claim to the corresponding LoA.
     * The details are available in
     * <a href="https://docs.google.com/document/d/1b-Mlet3Lq7qKLEf1BnHJ4nL1fq-vMe7fzpXyrq2wp08/edit">REFEDs
     * OIDCre</a> and in various AARC policies,
     * <a href="https://aarc-project.eu/guidelines/aarc-g021/">AARC-G021</a> and
     * <a href="https://aarc-project.eu/guidelines/aarc-g041/">AARC-G041</a> in
     * particular.
     */
    private static final Map<String,LoA> EDUPERSON_ASSURANCE = ImmutableMap.<String,LoA>builder()
            // REFEDS RAF policies
            .put("https://refeds.org/assurance/ID/unique", LoA.REFEDS_ID_UNIQUE)
            .put("https://refeds.org/assurance/ID/eppn-unique-no-reassign", LoA.REFEDS_ID_EPPN_UNIQUE_NO_REASSIGN)
            .put("https://refeds.org/assurance/ID/eppn-unique-reassign-1y", LoA.REFEDS_ID_EPPN_UNIQUE_REASSIGN_1Y)
            .put("https://refeds.org/assurance/IAP/low", LoA.REFEDS_IAP_LOW)
            .put("https://refeds.org/assurance/IAP/medium", LoA.REFEDS_IAP_MEDIUM)
            .put("https://refeds.org/assurance/IAP/high", LoA.REFEDS_IAP_HIGH)
            .put("https://refeds.org/assurance/IAP/local-enterprise", LoA.REFEDS_IAP_LOCAL_ENTERPRISE)
            .put("https://refeds.org/assurance/ATP/ePA-1m", LoA.REFEDS_ATP_1M)
            .put("https://refeds.org/assurance/ATP/ePA-1d", LoA.REFEDS_ATP_1D)
            .put("https://refeds.org/assurance/profile/cappuccino", LoA.REFEDS_PROFILE_CAPPUCCINO)
            .put("https://refeds.org/assurance/profile/espresso", LoA.REFEDS_PROFILE_ESPRESSO)

            // IGTF policies  see https://www.igtf.net/ap/authn-assurance/
            .put("https://igtf.net/ap/authn-assurance/aspen", LoA.IGTF_LOA_ASPEN)
            .put("https://igtf.net/ap/authn-assurance/birch", LoA.IGTF_LOA_BIRCH)
            .put("https://igtf.net/ap/authn-assurance/cedar", LoA.IGTF_LOA_CEDAR)
            .put("https://igtf.net/ap/authn-assurance/dogwood", LoA.IGTF_LOA_DOGWOOD)

            // AARC policies see https://aarc-project.eu/guidelines/#policy
            .put("https://aarc-project.eu/policy/authn-assurance/assam", LoA.AARC_PROFILE_ASSAM)

            // EGI policies see https://wiki.egi.eu/wiki/AAI_guide_for_SPs#Level_of_Assurance
            .put("https://aai.egi.eu/LoA#Low", LoA.EGI_LOW)
            .put("https://aai.egi.eu/LoA#Substantial", LoA.EGI_SUBSTANTIAL)
            .put("https://aai.egi.eu/LoA#High", LoA.EGI_HIGH)
            .build();

    private final ExecutorService executor;

    private final Map<URI,IdentityProvider> providersByIssuer;
    private final LoadingCache<IdentityProvider, JsonNode> discoveryCache;
    private final LoadingCache<String,List<LookupResult>> userInfoCache;
    private final Random random = new Random();
    private final JsonHttpClient jsonHttpClient;
    private final Duration slowLookupThreshold;

    public OidcAuthPlugin(Properties properties)
    {
        this(properties, buildClientFromProperties(properties));
    }

    private static JsonHttpClient buildClientFromProperties(Properties properties)
    {
        int soTimeout = (int)TimeUnit.valueOf(properties.getProperty(HTTP_TIMEOUT_UNIT))
                .toMillis(asInt(properties, HTTP_TIMEOUT));

        return new JsonHttpClient(asInt(properties, HTTP_CONCURRENT_ACCESS),
                asInt(properties, HTTP_PER_ROUTE_CONCURRENT_ACCESS),
                soTimeout);
    }

    private static int asInt(Properties properties, String key)
    {
        try {
            String value = properties.getProperty(key);
            checkArgument(value != null, "Missing " + key + " property");
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bad " + key + "value: " + e.getMessage());
        }
    }


    @Override
    public void stop()
    {
        executor.shutdownNow();
    }

    @VisibleForTesting
    OidcAuthPlugin(Properties properties, JsonHttpClient client)
    {
        Set<IdentityProvider> providers = new HashSet<>();
        providers.addAll(buildHosts(properties));
        providers.addAll(buildProviders(properties));
        checkArgument(!providers.isEmpty(), "No OIDC providers configured");

        int concurrency = asInt(properties, CONCURRENCY);
        executor = new BoundedCachedExecutor(concurrency);

        providersByIssuer = providers.stream().collect(toMap(IdentityProvider::getIssuerEndpoint, p -> p));
        jsonHttpClient = client;
        discoveryCache = createDiscoveryCache(asInt(properties, DISCOVERY_CACHE_REFRESH),
                TimeUnit.valueOf(properties.getProperty(DISCOVERY_CACHE_REFRESH_UNIT)));
        slowLookupThreshold = Duration.of(asInt(properties, HTTP_SLOW_LOOKUP),
                ChronoUnit.valueOf(properties.getProperty(HTTP_SLOW_LOOKUP_UNIT)));
        userInfoCache = createUserInfoCache( asInt(properties, ACCESS_TOKEN_CACHE_SIZE),
                asInt(properties, ACCESS_TOKEN_CACHE_REFRESH),
                TimeUnit.valueOf(properties.getProperty(ACCESS_TOKEN_CACHE_REFRESH_UNIT)),
                asInt(properties, ACCESS_TOKEN_CACHE_EXPIRE),
                TimeUnit.valueOf(properties.getProperty(ACCESS_TOKEN_CACHE_EXPIRE_UNIT)));
    }

    private static Set<IdentityProvider> buildHosts(Properties properties)
    {
        String oidcHostnamesProperty = properties.getProperty(OIDC_HOSTNAMES);
        checkArgument(oidcHostnamesProperty != null, OIDC_HOSTNAMES + " not defined");

        Map<Boolean, Set<String>> validHosts = Arrays.stream(oidcHostnamesProperty.split("\\s+"))
                                                     .filter(not(String::isEmpty))
                                                     .collect(
                                                             Collectors.groupingBy(InternetDomainName::isValid,
                                                                     Collectors.toSet())
                                                             );

        Set<String> badHosts = validHosts.get(Boolean.FALSE);
        checkArgument(badHosts == null, "Invalid hosts in %s: %s",
                OIDC_HOSTNAMES, Joiner.on(", ").join(nullToEmpty(badHosts)));

        Set<String> goodHosts = validHosts.get(Boolean.TRUE);
        return goodHosts == null
                ? Collections.emptySet()
                : goodHosts.stream()
                        .map(h -> new IdentityProvider(h, "https://" + h + "/"))
                        .collect(Collectors.toSet());
    }

    private static Set<IdentityProvider> buildProviders(Properties properties)
    {
        return properties.stringPropertyNames().stream()
                .filter(n -> n.startsWith(OIDC_PROVIDER_PREFIX))
                .map(n -> {
                            try {
                                return new IdentityProvider(n.substring(OIDC_PROVIDER_PREFIX.length()), properties.getProperty(n));
                            } catch (IllegalArgumentException e) {
                                throw new IllegalArgumentException("Bad OIDC provider " + n + ": " + e.getMessage());
                            }
                        })
                .collect(Collectors.toSet());
    }

    private LoadingCache<IdentityProvider, JsonNode> createDiscoveryCache(int refresh, TimeUnit refreshUnits)
    {
        return CacheBuilder.newBuilder()
                           .maximumSize(100)
                           .refreshAfterWrite(refresh, refreshUnits)
                           .build(
                                   new CacheLoader<IdentityProvider, JsonNode>() {
                                       @Override
                                       public JsonNode load(IdentityProvider provider) throws OidcException, IOException
                                       {
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
                                       public ListenableFuture<JsonNode> reload(final IdentityProvider provider, JsonNode value)
                                       {
                                           ListenableFutureTask<JsonNode> task = ListenableFutureTask.create(() -> load(provider));
                                           executor.execute(task);
                                           return task;
                                       }
                                   }
                                 );
    }

    private LoadingCache<String,List<LookupResult>> createUserInfoCache(int size,
            int refresh, TimeUnit refreshUnits, int expire, TimeUnit expireUnits)
    {
        return CacheBuilder.newBuilder()
                .maximumSize(size)
                .refreshAfterWrite(refresh, refreshUnits)
                .expireAfterWrite(expire, expireUnits)
                .build(new CacheLoader<String, List<LookupResult>>()
                        {
                            private ListenableFuture<List<LookupResult>> asyncFetch(String token)
                                    throws AuthenticationException
                            {
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
                                    throws InterruptedException, AuthenticationException
                            {
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
                                    List<LookupResult> results) throws AuthenticationException
                            {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Refreshing user-info for token {}", describe(token, 20));
                                }
                                return asyncFetch(token);
                            }
                       });
    }

    private static <T> Predicate<T> not(Predicate<T> t)
    {
        return t.negate();
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
                             Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals)
            throws AuthenticationException
    {
        Stopwatch userinfoLookupTiming = Stopwatch.createStarted();

        String token = null;
        for (Object credential : privateCredentials) {
            if (credential instanceof BearerTokenCredential) {
                checkAuthentication(token == null, "Multiple bearer tokens");

                token = ((BearerTokenCredential) credential).getToken();
                LOG.debug("Found bearer token: {}", token);
            }
        }

        checkAuthentication(token != null, "No bearer token in the credentials");

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

        List<LookupResult> successfulResults = allResults.stream().filter(LookupResult::isSuccess).collect(Collectors.toList());

        if (successfulResults.isEmpty()) {
            if (allResults.size() == 1) {
                LookupResult result = allResults.get(0);
                throw new AuthenticationException("OpenId Validation failed for " + result.getIdentityProvider().getName() + ": " + result.getError());
            } else {
                String randomId = randomId();
                String errors = allResults.stream()
                        .map(r -> "[" + r.getIdentityProvider().getName() + ": " + r.getError() + "]")
                        .collect(Collectors.joining(", "));
                LOG.warn("OpenId Validation Failure ({}): {}", randomId, errors);
                throw new AuthenticationException("OpenId Validation Failed check [log entry #" + randomId + "]");
            }
        }

        if (successfulResults.size() > 1 && LOG.isWarnEnabled()) {
            String names = successfulResults.stream()
                    .map(LookupResult::getIdentityProvider)
                    .map(IdentityProvider::getName)
                    .collect(Collectors.joining(", "));
            LOG.warn("Multiple OpenID-Connect endpoints accepted access token: {}", names);
        }
        successfulResults.stream()
                .map(LookupResult::getPrincipals)
                .forEach(identifiedPrincipals::addAll);
    }

    private LookupResult queryUserInfo(IdentityProvider ip, String token)
    {
        Stopwatch userinfoLookupTiming = null;

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Starting querying {} about token {}", ip.getName(), describe(token, 20));
            }

            JsonNode discoveryDoc = discoveryCache.get(ip);
            userinfoLookupTiming = Stopwatch.createStarted();
            String userInfoEndPoint = extractUserInfoEndPoint(discoveryDoc);
            Set<Principal> principals = validateBearerTokenWithOpenIdProvider(token, userInfoEndPoint);
            return LookupResult.success(ip, principals);
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

    private Collection<IdentityProvider> identityProviders(String token)
            throws AuthenticationException
    {
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

    private Set<Principal> validateBearerTokenWithOpenIdProvider(String token,
            String infoUrl) throws OidcException
    {
        try {
            JsonNode userInfo = getUserInfo(infoUrl, token);
            if (userInfo != null && userInfo.has("sub")) {
                LOG.debug("UserInfo from OpenId Provider: {}", userInfo);
                Set<Principal> principals = new HashSet<>();
                addSub(userInfo, principals);
                addNames(userInfo, principals);
                addEmail(userInfo, principals);
                addGroups(userInfo, principals);
                addLoAs(userInfo, principals);
                return principals;
            } else {
                throw new OidcException("No OpendId \"sub\"");
            }
        } catch (IllegalArgumentException iae) {
            throw new OidcException("Error parsing UserInfo: " + iae.getMessage());
        } catch (AuthenticationException e) {
            throw new OidcException(e.getMessage());
        } catch (IOException e) {
            throw new OidcException("Failed to fetch UserInfo: " + e.getMessage());
        }
    }

    private JsonNode getUserInfo(String url, String token) throws AuthenticationException, IOException
    {
        JsonNode userInfo = jsonHttpClient.doGetWithToken(url, token);
        if (userInfo.has("error")) {
            String error = userInfo.get("error").asText();
            String errorDescription = userInfo.get("error_description").asText();
            throw new AuthenticationException("Error: [" + error + ", " + errorDescription + " ]");
        } else {
            return userInfo;
        }
    }

    private String extractUserInfoEndPoint(JsonNode discoveryDoc)
    {
        if (discoveryDoc.has("userinfo_endpoint")) {
            return discoveryDoc.get("userinfo_endpoint").asText();
        } else {
            return null;
        }
    }

    private void addEmail(JsonNode userInfo, Set<Principal> principals)
    {
        if (userInfo.has("email")) {
            principals.add(new EmailAddressPrincipal(userInfo.get("email").asText()));
        }
    }

    private void addNames(JsonNode userInfo, Set<Principal> principals)
    {
        JsonNode givenName = userInfo.get("given_name");
        JsonNode familyName = userInfo.get("family_name");
        JsonNode fullName = userInfo.get("name");

        if (fullName != null && !fullName.asText().isEmpty()) {
            principals.add(new FullNamePrincipal(fullName.asText()));
        } else if (givenName != null && !givenName.asText().isEmpty()
                && familyName != null && !familyName.asText().isEmpty()) {
            principals.add(new FullNamePrincipal(givenName.asText(), familyName.asText()));
        }
    }

    private boolean addSub(JsonNode userInfo, Set<Principal> principals)
    {
        return principals.add(new OidcSubjectPrincipal(userInfo.get("sub").asText()));
    }

    private void addGroups(JsonNode userInfo, Set<Principal> principals)
    {
        if (userInfo.has("groups") && userInfo.get("groups").isArray()) {
            for (JsonNode group : userInfo.get("groups")) {
                principals.add(new OpenIdGroupPrincipal(group.asText()));
            }
        }
    }

    private void addLoAs(JsonNode userInfo, Set<Principal> principals)
    {
        if (userInfo.has("eduperson_assurance") && userInfo.get("eduperson_assurance").isArray()) {
            StreamSupport.stream(userInfo.get("eduperson_assurance").spliterator(), false)
                    .map(JsonNode::asText)
                    .map(EDUPERSON_ASSURANCE::get)
                    .filter(Objects::nonNull)
                    // FIXME we need to know when to accept REFEDS_IAP_LOCAL_ENTERPRISE.
                    .filter(l -> l != LoA.REFEDS_IAP_LOCAL_ENTERPRISE)
                    .map(LoAPrincipal::new)
                    .forEach(principals::add);
        }
    }

    private static <T> Collection<T> nullToEmpty(final Collection<T> collection)
    {
        return collection == null ? Collections.emptySet() : collection;
    }

    private String randomId()
    {
        byte[] rawId = new byte[6]; // a Base64 char represents 6 bits; 4 chars represent 3 bytes.
        random.nextBytes(rawId);
        return Base64.getEncoder().withoutPadding().encodeToString(rawId);
    }

    private static String describe(String id, int limit)
    {
        if (id.length() < limit) {
            return id;
        } else {
            int length = (limit-3)/2;
            return id.substring(0, length) + "..." + id.substring(id.length() - length);
        }
    }
}
