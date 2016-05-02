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
import com.google.common.util.concurrent.ListenableFuture;
import eu.emi.security.authn.x509.X509Credential;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

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
import org.dcache.srm.SrmRequest;
import org.dcache.srm.SrmResponse;
import org.dcache.srm.util.Axis;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfTExtraInfo;
import org.dcache.srm.v2_2.SrmPingResponse;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.util.CertificateFactories;
import org.dcache.util.NetLoggerBuilder;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
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
        SrmRequest msg =
                new SrmRequest(login.getSubject(), login.getLoginAttributes(), credential, remoteHost,
                               requestName, request);
        try {
            CellPath address = mapRequest(request);
            ListenableFuture<SrmResponse> future =
                    (address == null)
                    ? srmManagerStub.send(msg, SrmResponse.class)
                    : srmManagerStub.send(address, msg, SrmResponse.class);
            return mapResponse(future.get());
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), SRMException.class);
            Throwables.propagateIfInstanceOf(e.getCause(), CacheException.class);
            Throwables.propagateIfInstanceOf(e.getCause(), NoRouteToCellException.class);
            throw Throwables.propagate(e.getCause());
        }
    }

    private CellPath mapRequest(Object request) throws SRMInternalErrorException
    {
        Optional<Field> field = requestTokenFieldCache.getUnchecked(request.getClass());
        if (field.isPresent()) {
            try {
                Field f = field.get();
                String token = (String) f.get(request);
                if (token != null && token.length() > 8) {
                    f.set(request, token.substring(8));

                    String path = SrmService.getZooKeeperBackendPath(token.substring(0, 8));
                    ChildData data = backends.getCurrentData(path);
                    if (data == null) {
                        throw new SRMInternalErrorException("SRM backend serving this request token is currently offline.");
                    }
                    return new CellPath(new String(data.getData(), US_ASCII));
                }
            } catch (IllegalAccessException e) {
                Throwables.propagate(e);
            }
        }
        return null;
    }

    private Object mapResponse(SrmResponse response)
    {
        Object o = response.getResponse();
        Optional<Field> field = requestTokenFieldCache.getUnchecked(o.getClass());
        field.ifPresent(f -> {
            try {
                f.set(o, response.getId() + f.get(o));
            } catch (IllegalAccessException e) {
                Throwables.propagate(e);
            }
        });
        return o;
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
