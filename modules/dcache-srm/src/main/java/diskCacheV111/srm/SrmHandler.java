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

package diskCacheV111.srm;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import eu.emi.security.authn.x509.X509Credential;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.security.auth.Subject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.zookeeper.PathChildrenCache;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.cells.CellStub;
import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.commons.stats.rrd.RrdRequestCounters;
import org.dcache.commons.stats.rrd.RrdRequestExecutionTimeGauges;
import org.dcache.srm.SRMAuthenticationException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SrmRequest;
import org.dcache.srm.SrmResponse;
import org.dcache.srm.util.Axis;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTExtraInfo;
import org.dcache.srm.v2_2.ArrayOfTRequestSummary;
import org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn;
import org.dcache.srm.v2_2.SrmGetRequestSummaryRequest;
import org.dcache.srm.v2_2.SrmGetRequestSummaryResponse;
import org.dcache.srm.v2_2.SrmGetRequestTokensResponse;
import org.dcache.srm.v2_2.SrmPingResponse;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TRequestSummary;
import org.dcache.srm.v2_2.TRequestTokenReturn;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.util.CertificateFactories;
import org.dcache.util.NetLoggerBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.*;
import static org.dcache.srm.v2_2.TStatusCode.*;

/**
 * Utility class to submit requests to the SRM backend service.
 */
public class SrmHandler implements CellInfoProvider, CuratorFrameworkAware
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SrmHandler.class);

    private static final Set<TStatusCode> FAILURES =
            ImmutableSet.of(SRM_FAILURE,
                            SRM_AUTHENTICATION_FAILURE, SRM_AUTHORIZATION_FAILURE, SRM_INVALID_REQUEST, SRM_INVALID_PATH,
                            SRM_FILE_LIFETIME_EXPIRED, SRM_SPACE_LIFETIME_EXPIRED, SRM_EXCEED_ALLOCATION, SRM_NO_USER_SPACE,
                            SRM_NO_FREE_SPACE, SRM_DUPLICATION_ERROR, SRM_NON_EMPTY_DIRECTORY, SRM_TOO_MANY_RESULTS,
                            SRM_INTERNAL_ERROR, SRM_FATAL_INTERNAL_ERROR, SRM_NOT_SUPPORTED, SRM_ABORTED,
                            SRM_REQUEST_TIMED_OUT, SRM_FILE_BUSY, SRM_FILE_LOST, SRM_FILE_UNAVAILABLE, SRM_CUSTOM_STATUS);

    private final RequestLogger[] loggers =
            { new RequestExecutionTimeGaugeLogger(), new CounterLogger(), new AccessLogger() };

    private final RequestCounters<Class<?>> srmServerCounters = new RequestCounters<>("srmv2");
    private final RequestExecutionTimeGauges<Class<?>> srmServerGauges = new RequestExecutionTimeGauges<>("srmv2");

    private final CertificateFactory cf = CertificateFactories.newX509CertificateFactory();

    private final LoadingCache<Class, Optional<Field>> requestTokenFieldCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<Class, Optional<Field>>()
            {
                @Override
                public Optional<Field> load(Class clazz)
                {
                    try {
                        Field field = clazz.getDeclaredField("requestToken");
                        field.setAccessible(true);
                        return Optional.of(field);
                    } catch (NoSuchFieldException e) {
                        return Optional.empty();
                    }
                }
            });

    private PathChildrenCache backends;

    private String counterRrdDirectory;

    private String gaugeRrdDirectory;

    private boolean isClientDNSLookup;

    private LoginStrategy loginStrategy;

    private CellStub srmManagerStub;

    private ArrayOfTExtraInfo pingExtraInfo;
    private CuratorFramework client;

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        this.client = client;
    }

    public void setCounterRrdDirectory(String counterRrdDirectory)
    {
        this.counterRrdDirectory = counterRrdDirectory;
    }

    public void setGaugeRrdDirectory(String gaugeRrdDirectory)
    {
        this.gaugeRrdDirectory = gaugeRrdDirectory;
    }

    public void setClientDNSLookup(boolean clientDNSLookup)
    {
        isClientDNSLookup = clientDNSLookup;
    }

    @Required
    public void setSrmManagerStub(CellStub srmManagerStub)
    {
        this.srmManagerStub = srmManagerStub;
    }

    @Required
    public void setLoginStrategy(LoginStrategy loginStrategy)
    {
        this.loginStrategy = loginStrategy;
    }

    @Required
    public void setPingExtraInfo(ImmutableMap<String, String> pingExtraInfo)
    {
        this.pingExtraInfo = buildExtraInfo(pingExtraInfo);
    }

    @PostConstruct
    public void init() throws Exception
    {
        if (!Strings.isNullOrEmpty(counterRrdDirectory)) {
            String rrddir = counterRrdDirectory + File.separatorChar + "srmv2";
            RrdRequestCounters<?> rrdSrmServerCounters =
                    new RrdRequestCounters<>(srmServerCounters, rrddir);
            rrdSrmServerCounters.startRrdUpdates();
            rrdSrmServerCounters.startRrdGraphPlots();
        }

        if (!Strings.isNullOrEmpty(gaugeRrdDirectory)) {
            File rrddir = new File(gaugeRrdDirectory + File.separatorChar + "srmv2");
            RrdRequestExecutionTimeGauges<?> rrdSrmServerGauges =
                    new RrdRequestExecutionTimeGauges<>(srmServerGauges, rrddir);
            rrdSrmServerGauges.startRrdUpdates();
            rrdSrmServerGauges.startRrdGraphPlots();
        }

        backends = new PathChildrenCache(client, "/dcache/srm/backends", true);
        backends.start();
    }

    @PreDestroy
    public void shutdown() throws IOException
    {
        if (backends != null) {
            backends.close();
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println(srmServerCounters);
        pw.println(srmServerGauges);
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    private ArrayOfTExtraInfo buildExtraInfo(Map<String,String> items)
    {
        if (items.isEmpty()) {
            return null;
        }

        TExtraInfo[] extraInfo = new TExtraInfo[items.size()];
        int i = 0;
        for (Map.Entry<String,String> item : items.entrySet()) {
            extraInfo [i++] = new TExtraInfo(item.getKey(), Strings.emptyToNull(item.getValue()));
        }

        return new ArrayOfTExtraInfo(extraInfo);
    }

    public Object handleRequest(String requestName, Object request)  throws RemoteException
    {
        long startTimeStamp = System.currentTimeMillis();
        // requestName values all start "srm".  This is redundant, so may
        // be removed when creating the session id.  The initial character is
        // converted to lowercase, so "srmPrepareToPut" becomes "prepareToPut".
        String session = "srm2:" +
                Character.toLowerCase(requestName.charAt(3)) +
                requestName.substring(4);
        try (JDC ignored = JDC.createSession(session)) {
            for (RequestLogger logger : loggers) {
                logger.request(requestName, request);
            }

            Subject user = null;
            Object response;
            if (requestName.equals("srmPing")) {
                // Ping is special as it isn't authenticated and unable to return a failure
                response = new SrmPingResponse("v2.2", pingExtraInfo);
            } else {
                try {
                    LoginReply login = login();
                    user = login.getSubject();
                    X509Credential credential = Axis.getDelegatedCredential().orElse(null);
                    String remoteIP = Axis.getRemoteAddress();
                    String remoteHost = isClientDNSLookup ?
                                        InetAddresses.forString(remoteIP).getCanonicalHostName() : remoteIP;

                    response = dispatch(login, credential, remoteHost, requestName, request);
                } catch (SRMInternalErrorException e) {
                    LOGGER.error(e.getMessage());
                    response = getFailedResponse(requestName, e.getStatusCode(),
                                                 "Authentication failed (server log contains additional information).");
                } catch (SRMAuthorizationException e) {
                    LOGGER.info(e.getMessage());
                    response = getFailedResponse(requestName, e.getStatusCode(), "Permission denied.");
                } catch (SRMAuthenticationException e) {
                    LOGGER.warn(e.getMessage());
                    response = getFailedResponse(requestName, e.getStatusCode(),
                                                 "Authentication failed (server log contains additional information).");
                } catch (SRMException e) {
                    response = getFailedResponse(requestName, e.getStatusCode(), e.getMessage());
                } catch (PermissionDeniedCacheException e) {
                    response = getFailedResponse(requestName, TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
                } catch (CacheException e) {
                    response = getFailedResponse(requestName, TStatusCode.SRM_INTERNAL_ERROR, e.getMessage());
                } catch (InterruptedException e) {
                    response = getFailedResponse(requestName, TStatusCode.SRM_FATAL_INTERNAL_ERROR, "Server shutdown.");
                } catch (NoRouteToCellException e) {
                    LOGGER.error(e.getMessage());
                    response = getFailedResponse(requestName, TStatusCode.SRM_INTERNAL_ERROR,
                                                 "SRM backend serving this request is currently offline.");
                }
            }
            long time = System.currentTimeMillis() - startTimeStamp;
            for (RequestLogger logger : loggers) {
                logger.response(requestName, request, response, user, time);
            }
            return response;
        }
    }

    private LoginReply login() throws SRMAuthenticationException, CacheException, SRMInternalErrorException
    {
        try {
            Subject subject = new Subject();
            X509Certificate[] chain = Axis.getCertificateChain().orElseThrow(
                    () -> new SRMAuthenticationException("Client's certificate chain is missing from request"));
            subject.getPublicCredentials().add(cf.generateCertPath(asList(chain)));
            subject.getPrincipals().add(new Origin(InetAddresses.forString(Axis.getRemoteAddress())));
            return loginStrategy.login(subject);
        } catch (CertificateException e) {
            throw new SRMInternalErrorException("Failed to process certificate chain.", e);
        }
    }

    private Object dispatch(LoginReply login, X509Credential credential, String remoteHost,
                            String requestName, Object request)
            throws CacheException, InterruptedException, SRMException, NoRouteToCellException
    {
        Function<Object,SrmRequest> toMessage =
                req -> new SrmRequest(login.getSubject(), login.getLoginAttributes(), credential,
                                      remoteHost, requestName, req);
        try {
            switch (requestName) {
            case "srmGetRequestTokens": {
                List<ListenableFuture<SrmResponse>> futures =
                        backends.getCurrentData().stream()
                                .map(this::toCellPath)
                                .map(path -> srmManagerStub.send(path, toMessage.apply(request), SrmResponse.class))
                                .collect(toList());
                return mapGetRequestTokensResponse(Futures.allAsList(futures).get());
            }
            case "srmGetRequestSummary": {
                SrmGetRequestSummaryRequest summaryRequest = (SrmGetRequestSummaryRequest) request;
                String[] requestTokens = summaryRequest.getArrayOfRequestTokens().getStringArray();
                if (requestTokens == null || requestTokens.length == 0) {
                    throw new SRMInvalidRequestException("arrayOfRequestTokens is empty");
                }
                Map<String, ListenableFuture<TRequestSummary>> futureMap =
                        provideRequestSummary(toMessage, summaryRequest.getAuthorizationID(), requestTokens);
                return toGetRequestSummaryResponse(futureMap);
            }
            default: {
                try (MappedRequest mapped = mapRequest(request)) {
                    ListenableFuture<SrmResponse> future =
                            (mapped == null)
                            ? srmManagerStub.send(toMessage.apply(request), SrmResponse.class)
                            : srmManagerStub.send(mapped.getBackend(), toMessage.apply(mapped.getRequest()), SrmResponse.class);
                    return mapResponse(future.get());
                }
            }
            }
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), SRMException.class);
            Throwables.propagateIfInstanceOf(e.getCause(), CacheException.class);
            Throwables.propagateIfInstanceOf(e.getCause(), NoRouteToCellException.class);
            throw Throwables.propagate(e.getCause());
        }
    }

    private SrmGetRequestSummaryResponse toGetRequestSummaryResponse(
            Map<String, ListenableFuture<TRequestSummary>> futureMap)
            throws InterruptedException, CacheException, NoRouteToCellException
    {
        boolean hasFailure = false;
        boolean hasSuccess = false;
        List<TRequestSummary> summaries = new ArrayList<>();
        for (Map.Entry<String, ListenableFuture<TRequestSummary>> entry : futureMap.entrySet()) {
            try {
                summaries.add(entry.getValue().get());
                hasSuccess = true;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof SRMException) {
                    summaries.add(createRequestSummaryFailure(
                            entry.getKey(), ((SRMException) cause).getStatusCode(), cause.getMessage()));
                    hasFailure = true;
                } else {
                    Throwables.propagateIfInstanceOf(cause, CacheException.class);
                    Throwables.propagateIfInstanceOf(cause, NoRouteToCellException.class);
                    throw Throwables.propagate(cause);
                }
            }
        }

        TReturnStatus status;
        if (!hasFailure) {
            status = new TReturnStatus(SRM_SUCCESS, "All request statuses have been retrieved.");
        } else if (hasSuccess) {
            status = new TReturnStatus(SRM_PARTIAL_SUCCESS, "Some request statuses have been retrieved.");
        } else {
            status = new TReturnStatus(SRM_FAILURE, "No request statuses have been retrieved.");
        }
        return new SrmGetRequestSummaryResponse(
                status, new ArrayOfTRequestSummary(summaries.stream().toArray(TRequestSummary[]::new)));
    }

    /**
     * Provide request summaries for a collection of request tokens. The result is
     * provided as a map from request token to a future request summary as the result is
     * typically fetched asynchronously from the backends.
     */
    private Map<String, ListenableFuture<TRequestSummary>> provideRequestSummary(
            Function<Object,SrmRequest> toMessage, String authorizationId, String[] tokens)
    {
        Function<String, Optional<CellPath>> optionalBackendOf =
                token -> Optional.ofNullable(hasPrefix(token) ? backendOf(token) : null);
        Map<Optional<CellPath>, Set<String>> tokensByBackend =
                Stream.of(tokens).collect(groupingBy(optionalBackendOf, toSet()));

        return tokensByBackend.entrySet().stream()
                .map(e -> e.getKey().isPresent()
                          ? provideRequestSummaryForTokenWithBackend(toMessage, authorizationId, e.getValue(), e.getKey().get())
                          : provideRequestSummaryForTokenWithoutBackend(e.getValue()))
                .flatMap(m -> m.entrySet().stream())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Returns a map of responses for a list of tokens that cannot be mapped to a backend.
     */
    private Map<String, ? extends ListenableFuture<TRequestSummary>> provideRequestSummaryForTokenWithoutBackend(
            Collection<String> tokens)
    {
        return tokens.stream().collect(toMap(Function.identity(), this::provideRequestSummaryForTokenWithoutBackend));
    }

    /**
     * Returns a response for a token that cannot be mapped to a backend. The response depends
     * on whether the token has a valid backend id prefix (backend if offline) or not (token is invalid).
     */
    private ListenableFuture<TRequestSummary> provideRequestSummaryForTokenWithoutBackend(String token)
    {
        if (!hasPrefix(token)) {
            return Futures.immediateFailedFuture(new SRMInvalidRequestException("No such request token: " + token));
        }
        return Futures.immediateFailedFuture(
                new SRMInvalidRequestException("Backend for request " + token + " is currently unavailable."));
    }

    /**
     * Returns a map of summaries from backends for a list of tokens.
     */
    private Map<String, ? extends ListenableFuture<TRequestSummary>> provideRequestSummaryForTokenWithBackend(
            Function<Object, SrmRequest> toMessage, String authorizationId, Collection<String> tokens, CellPath backend)
    {
        SrmRequest msg = toMessage.apply(createRequestSummaryRequest(authorizationId, tokens));
        ListenableFuture<SrmResponse> futureResponse = srmManagerStub.send(backend, msg, SrmResponse.class);
        return transformToMap(tokens, futureResponse, r -> toRequestSummaries(tokens, r), this::translateBackendError);
    }

    /**
     * Injects a summary into a settable future, translating errors that indicate failure to obtain
     * a summary to a failure future.
     */
    private void translateBackendError(TRequestSummary summary, SettableFuture<TRequestSummary> future)
    {
        TStatusCode statusCode = summary.getStatus().getStatusCode();
        if (statusCode == SRM_INVALID_REQUEST) {
            future.setException(new SRMInvalidRequestException(
                    "No such request token: " + summary.getRequestToken()));
        } else if (statusCode == SRM_FAILURE && summary.getRequestType() == null) {
            future.setException(new SRMException(summary.getStatus().getExplanation()));
        } else {
            future.set(summary);
        }
    }

    /**
     * Returns a backend request summary request for a list of tokens.
     */
    private SrmGetRequestSummaryRequest createRequestSummaryRequest(String authorizationId, Collection<String> tokens)
    {
        String[] backendTokens = tokens.stream().map(SrmHandler::backendTokenOf).toArray(String[]::new);
        return new SrmGetRequestSummaryRequest(new ArrayOfString(backendTokens), authorizationId);
    }

    /**
     * Returns a map of request summaries for a backend response.
     */
    private Map<String, TRequestSummary> toRequestSummaries(Collection<String> tokens, SrmResponse response)
    {
        TRequestSummary[] summaries = ((SrmGetRequestSummaryResponse) response.getResponse()).getArrayOfRequestSummaries().getSummaryArray();
        Map<String, TRequestSummary> summaryByBackendToken =
                Stream.of(summaries).collect(toMap(TRequestSummary::getRequestToken, Function.identity()));
        return tokens.stream()
                .collect(toMap(token -> token,
                               token -> mapRequestSummary(token, summaryByBackendToken.get(backendTokenOf(token)))));
    }

    /**
     * Map a backend request summary to a frontend request summary.
     */
    private TRequestSummary mapRequestSummary(String token, TRequestSummary summary)
    {
        return (summary == null)
               ? createRequestSummaryFailure(token, SRM_INVALID_REQUEST, "No such request token: " + token)
               : new TRequestSummary(token, summary.getStatus(), summary.getRequestType(),
                                     summary.getTotalNumFilesInRequest(), summary.getNumOfCompletedFiles(),
                                     summary.getNumOfWaitingFiles(), summary.getNumOfFailedFiles());
    }

    private static TRequestSummary createRequestSummaryFailure(String token, TStatusCode status, String explanation)
    {
        return new TRequestSummary(token, new TReturnStatus(status, explanation),
                                   null, null, null, null, null);
    }

    /**
     * Transforms a future to a map of future values.
     *
     * The return map contains {@code keys} elements. Each key is mapped to a future value. If
     * {@code future} fails, all returned futures fail with the same error. Otherwise {@code mapper}
     * maps the return value of {@code future} to a map of values. {@code acceptor} is called for
     * each value, applying the value to the settable future in the returned map.
     *
     * @param keys The keys of the map to return.
     * @param future The future providing the input value.
     * @param mapper A function that maps the future input to a map of output values.
     * @param applicator Consumer that applies an output value to a future output value.
     */
    private static <K, T, V> Map<K, ? extends ListenableFuture<V>> transformToMap(
            Collection<K> keys, ListenableFuture<T> future,
            Function<T, Map<K, V>> mapper,
            BiConsumer<V, SettableFuture<V>> applicator)
    {
        Map<K, SettableFuture<V>> result = keys.stream().collect(toMap(key -> key, key -> SettableFuture.create()));
        Futures.addCallback(future, new FutureCallback<T>()
        {
            @Override
            public void onSuccess(T t)
            {
                Map<K, V> map = mapper.apply(t);
                result.forEach((key, f) -> applicator.accept(map.get(key), f));
            }

            @Override
            public void onFailure(Throwable t)
            {
                result.values().forEach(f -> f.setException(t));
            }
        });
        return result;
    }

    private CellPath toCellPath(ChildData data)
    {
        return new CellPath(new String(data.getData(), US_ASCII));
    }

    /**
     * Encapsulates the result of mapping a request to a backend.
     *
     * The request is modified inline to reflect the internal request token. This
     * avoids copying the request, however the modification must be undone by
     * closing this MappedRequest once the mapped request is no longer needed.
     */
    private class MappedRequest implements AutoCloseable
    {
        private final Object request;

        private final CellPath backend;

        private final Field field;

        private final String token;

        MappedRequest(Object request, CellPath backend, Field field, String token) throws IllegalAccessException
        {
            this.request = request;
            this.backend = backend;
            this.field = field;
            this.token = token;
            field.set(request, backendTokenOf(token));
        }

        CellPath getBackend()
        {
            return backend;
        }

        Object getRequest()
        {
            return request;
        }

        @Override
        public void close()
        {
            try {
                field.set(request, token);
            } catch (IllegalAccessException e) {
                Throwables.propagate(e);
            }
        }
    }

    private MappedRequest mapRequest(Object request) throws SRMInternalErrorException
    {
        Optional<Field> field = requestTokenFieldCache.getUnchecked(request.getClass());
        if (field.isPresent()) {
            try {
                Field f = field.get();
                String token = (String) f.get(request);
                if (hasPrefix(token)) {
                    CellPath path = backendOf(token);
                    if (path == null) {
                        throw new SRMInternalErrorException("SRM backend serving this request token is currently offline.");
                    }
                    return new MappedRequest(request, path, f, token);
                }
            } catch (IllegalAccessException e) {
                Throwables.propagate(e);
            }
        }
        return null;
    }

    private SrmGetRequestTokensResponse mapGetRequestTokensResponse(List<SrmResponse> responses)
    {
        List<TRequestTokenReturn> tokens = new ArrayList<>();
        for (SrmResponse srmResponse : responses) {
            SrmGetRequestTokensResponse response =
                    (SrmGetRequestTokensResponse) srmResponse.getResponse();
            if (response.getReturnStatus().getStatusCode() != SRM_SUCCESS) {
                return response;
            }
            for (TRequestTokenReturn token : response.getArrayOfRequestTokens().getTokenArray()) {
                tokens.add(new TRequestTokenReturn(prefix(srmResponse.getId(), token.getRequestToken()),
                                                   token.getCreatedAtTime()));
            }
        }
        ArrayOfTRequestTokenReturn arrayOfRequestTokens = new ArrayOfTRequestTokenReturn(
                tokens.stream().toArray(TRequestTokenReturn[]::new));
        return new SrmGetRequestTokensResponse(new TReturnStatus(SRM_SUCCESS, "Request processed successfully."), arrayOfRequestTokens);
    }

    private Object mapResponse(SrmResponse response)
    {
        Object o = response.getResponse();
        Optional<Field> field = requestTokenFieldCache.getUnchecked(o.getClass());
        field.ifPresent(f -> {
            try {
                f.set(o, prefix(response.getId(), (String) f.get(o)));
            } catch (IllegalAccessException e) {
                Throwables.propagate(e);
            }
        });
        return o;
    }

    private CellPath backendOf(String prefixedToken)
    {
        checkArgument(hasPrefix(prefixedToken));
        String path = SrmService.getZooKeeperBackendPath(backendIdOf(prefixedToken));
        ChildData data = backends.getCurrentData(path);
        if (data != null) {
            return toCellPath(data);
        }
        return null;
    }

    private static String backendIdOf(String prefixedToken)
    {
        return prefixedToken.substring(0, 8);
    }

    private static String backendTokenOf(String prefixedToken)
    {
        checkArgument(hasPrefix(prefixedToken));
        return prefixedToken.substring(9);
    }

    private static boolean hasPrefix(String token)
    {
        return token != null && token.length() > 9 && token.charAt(8) == ':';
    }

    private static String prefix(String backend, String backendToken)
    {
        return backend + ":" + backendToken;
    }

    private Object getFailedResponse(String requestName, TStatusCode statusCode, String errorMessage)
            throws RemoteException
    {
        char first = requestName.charAt(0);
        String capitalizedRequestName =  Character.isUpperCase(first) ? requestName :
                                         (Character.toUpperCase(first) + requestName.substring(1));

        try {
            Class<?> responseClass = Class.forName("org.dcache.srm.v2_2."+capitalizedRequestName+"Response");
            Constructor<?> responseConstructor = responseClass.getConstructor();
            Object response;
            try {
                response = responseConstructor.newInstance();
            } catch (InvocationTargetException e) {
                Throwables.propagateIfPossible(e, Exception.class);
                throw new RuntimeException("Unexpected exception", e);
            }
            try {
                Method setReturnStatus = responseClass
                        .getMethod("setReturnStatus", TReturnStatus.class);
                setReturnStatus.setAccessible(true);
                try {
                    setReturnStatus.invoke(response, new TReturnStatus(statusCode, errorMessage));
                } catch (InvocationTargetException e) {
                    Throwables.propagateIfPossible(e, Exception.class);
                    throw new RuntimeException("Unexpected exception", e);
                }
            } catch (Exception e) {
                LOGGER.trace("getFailedResponse invocation failed", e);
                Method setStatusCode = responseClass.getMethod("setStatusCode", TStatusCode.class);
                setStatusCode.setAccessible(true);
                setStatusCode.invoke(response, statusCode);
                Method setExplanation = responseClass.getMethod("setExplanation", String.class);
                setExplanation.setAccessible(true);
                setExplanation.invoke(response, errorMessage);
            }
            return response;
        } catch (Exception e) {
            throw new RemoteException("Failed to generate SRM reply", e);
        }
    }

    private interface RequestLogger
    {
        void request(String requestName, Object request);
        void response(String requestName, Object request, Object response, Subject user, long time);
    }

    public class AccessLogger implements RequestLogger
    {
        private final Logger ACCESS_LOGGER = LoggerFactory.getLogger("org.dcache.access.srm");

        @Override
        public void request(String requestName, Object request)
        {
        }

        @Override
        public void response(String requestName, Object request, Object response, Subject user, long time)
        {
            if (ACCESS_LOGGER.isErrorEnabled()) {
                TReturnStatus status = getReturnStatus(response);
                boolean isFailure = status != null && FAILURES.contains(status.getStatusCode());
                if (!isFailure && !ACCESS_LOGGER.isInfoEnabled()) {
                    return;
                }

                NetLoggerBuilder.Level level = isFailure ? NetLoggerBuilder.Level.ERROR : NetLoggerBuilder.Level.INFO;
                NetLoggerBuilder log = new NetLoggerBuilder(level, "org.dcache.srm.request").omitNullValues();
                log.add("session", JDC.getSession());
                log.add("socket.remote", Axis.getRemoteSocketAddress());
                log.add("request.method", requestName);
                log.add("user.dn", Axis.getDN().orElse("-"));
                if (user != null) {
                    log.add("user.mapped", user);
                }
                String requestToken = getRequestToken(request, response);
                if (requestToken != null) {
                    log.add("request.token", requestToken);
                } else {
                    log.add("request.surl", getSurl(request));
                }
                if (status != null) {
                    log.add("status.code", status.getStatusCode());
                    log.add("status.explanation", status.getExplanation());
                }
                log.add("client-info", Axis.getRequestHeader("ClientInfo"));
                log.add("user-agent", Axis.getUserAgent());
                log.toLogger(ACCESS_LOGGER);
            }
        }
    }

    public class CounterLogger implements RequestLogger
    {
        @Override
        public void request(String requestName, Object request)
        {
            srmServerCounters.incrementRequests(request.getClass());
        }

        @Override
        public void response(String requestName, Object request, Object response, Subject user, long time)
        {
            TReturnStatus status = getReturnStatus(response);
            if (status != null && FAILURES.contains(status.getStatusCode())) {
                srmServerCounters.incrementFailed(request.getClass());
            }
        }
    }

    private class RequestExecutionTimeGaugeLogger implements RequestLogger
    {
        @Override
        public void request(String requestName, Object request)
        {
        }

        @Override
        public void response(String requestName, Object request, Object response, Subject user, long time)
        {
            srmServerGauges.update(request.getClass(), time);
        }
    }

    private static String getSurl(Object request)
    {
        try {
            Method getReturnStatus = request.getClass().getDeclaredMethod("getSURL");
            Class<?> returnType = getReturnStatus.getReturnType();
            if (org.apache.axis.types.URI.class.isAssignableFrom(returnType)) {
                Object uri = getReturnStatus.invoke(request);
                if (uri != null) {
                    return uri.toString();
                }
            }
        } catch (NoSuchMethodException e) {
            // Unfortunately, Java standard API provides no nice way of
            // discovering if a method exists by reflection.  This is perhaps
            // the least ugly.
        } catch (InvocationTargetException | IllegalAccessException e) {
            LOGGER.debug("Failed to extract SURL: {}", e.toString());
        }
        return null;
    }

    private static String getRequestToken(Object request, Object response)
    {
        String requestToken = getRequestToken(response);
        if (requestToken != null) {
            return requestToken;
        }
        requestToken = getRequestToken(request);
        if (requestToken != null) {
            return requestToken;
        }
        return null;
    }

    private static String getRequestToken(Object response)
    {
        try {
            Method getReturnStatus = response.getClass().getDeclaredMethod("getRequestToken");
            Class<?> returnType = getReturnStatus.getReturnType();
            if (String.class.isAssignableFrom(returnType)) {
                return (String) getReturnStatus.invoke(response);
            }
        } catch (NoSuchMethodException e) {
            // Unfortunately, Java standard API provides no nice way of
            // discovering if a method exists by reflection.  This is perhaps
            // the least ugly.
        } catch (InvocationTargetException | IllegalAccessException e) {
            LOGGER.debug("Failed to extract request token: {}", e.toString());
        }
        return null;
    }

    private static TReturnStatus getReturnStatus(Object response)
    {
        try {
            Method getReturnStatus = response.getClass().getDeclaredMethod("getReturnStatus");
            Class<?> returnType = getReturnStatus.getReturnType();
            if (TReturnStatus.class.isAssignableFrom(returnType)) {
                return (TReturnStatus) getReturnStatus.invoke(response);
            }
        } catch (NoSuchMethodException e) {
            // Unfortunately, Java standard API provides no nice way of
            // discovering if a method exists by reflection.  This is perhaps
            // the least ugly.
        } catch (InvocationTargetException | IllegalAccessException e) {
            LOGGER.debug("Failed to extract status code: {}", e.toString());
        }
        return null;
    }
}
