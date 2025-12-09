/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
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
package org.dcache.util.jetty;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.RateLimiter;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jetty handler collection that enforces per-client and global rate limiting,
 * as well as temporary blocking of clients that exceed error thresholds.
 * <p>
 * Each client IP is assigned a rate limiter and error counter. If a client exceeds
 * the allowed number of errors within a short time window, it is blocked for a fixed duration.
 * Requests are rejected with HTTP 429 if rate limits are exceeded or the client is blocked.
 * <p>
 * The handler uses Guava caches to manage rate limiters, error counters, and blocked clients,
 * automatically expiring idle entries.
 *
 * Based on original code by Sandro Grizzo.
 */
public class RateLimitedHandlerList extends HandlerCollection implements CellCommandListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitedHandlerList.class);

    /**
     * Initial capacity of the client IP rates limiters map size.
     */
    private final int CLIENT_IP_CACHE_INITIAL_CAPACITY = 1024;

    /**
     * Maximum number of errors allowed per client before blocking.
     */
    private int maxErrorsPerClient;

    /**
     * An object used as a value when client is blocked.
     */
    private final Object BLOCK = new Object();

    /**
     * Calculated per-client rate limit based on the global rate limit and factor.
     */
    private double perClientRate;

    /**
     * Rate limiter for all requests.
     */
    private final RateLimiter globalRateLimiter;

    /**
     * Cache mapping client identifiers (e.g., IP addresses) to their respective rate limiters.
     */
    private final Cache<String, RateLimiter> perClientRates ;

    /**
     * Cache mapping client identifiers to a blocking marker object for temporarily blocked clients.
     */
    private final Cache<String, Object> blockedClients;

    /**
     * Cache mapping client identifiers to their respective error counters.
     */
    private final Cache<String, AtomicInteger> perClientErrorCount;

    public static class Configuration {
        private int maxClientsToTrack;
        private long maxGlobalRequestsPerSecond;
        private int maxErrorsPerClient;
        private int perClientPercent;
        private long clientIdleTime;
        private ChronoUnit clientIdleTimeUnit;
        private long clientBlockingTime;
        private ChronoUnit clientBlockingTimeUnit;
        private long errorAcceptanceWindow;
        private ChronoUnit errorAcceptanceWindowUnit;

        public void setGlobalRequestsPerSecond(long value) {
            this.maxGlobalRequestsPerSecond = value;

        }

        public void setNumErrorsBeforeBlocking(int value) {
            this.maxErrorsPerClient = value;
        }

        public void setLimitPercentagePerClient(int value) {
            this.perClientPercent = value;
        }

        public void setClientIdleTime(long value) {
            this.clientIdleTime = value;
        }

        public void setClientBlockingTime(long clientBlockingTime) {
            this.clientBlockingTime = clientBlockingTime;
        }

        public void setErrorCountingWindow(long errorAcceptanceWindow) {
            this.errorAcceptanceWindow = errorAcceptanceWindow;
        }

        public void setClientIdleTimeUnit(ChronoUnit clientIdleTimeUnit) {
            this.clientIdleTimeUnit = clientIdleTimeUnit;
        }

        public void setClientBlockingTimeUnit(ChronoUnit clientBlockingTimeUnit) {
            this.clientBlockingTimeUnit = clientBlockingTimeUnit;
        }

        public void setErrorCountingWindowUnit(ChronoUnit errorAcceptanceWindowUnit) {
            this.errorAcceptanceWindowUnit = errorAcceptanceWindowUnit;
        }

        public void setMaxClientsToTrack(int maxClientsToTrack) {
            this.maxClientsToTrack = maxClientsToTrack;
        }
    }


    /** * Constructs a RateLimitedHandlerList with parameters from the given configuration.
     *
     * @param configuration the configuration object containing rate limiting and blocking parameters
     */
    public RateLimitedHandlerList(Configuration configuration) {
        this(configuration.maxClientsToTrack,
                configuration.maxGlobalRequestsPerSecond,
             configuration.maxErrorsPerClient,
             configuration.perClientPercent,
             Duration.of(configuration.clientIdleTime, configuration.clientIdleTimeUnit),
             Duration.of(configuration.clientBlockingTime, configuration.clientBlockingTimeUnit),
             Duration.of(configuration.errorAcceptanceWindow, configuration.errorAcceptanceWindowUnit));
    }

    /** * Constructs a RateLimitedHandlerList with specified rate limiting and blocking parameters.
     *
     * @param maxGlobalRequestsPerSecond maximum number of requests per second allowed globally
     * @param maxErrorsPerClient maximum number of errors allowed per client before blocking
     * @param perClientPercent percentage of the global rate limit to apply per client (1 < percent <= 100)
     * @param clientIdleTime duration after which an idle client's rate limiter is removed
     * @param clientBlockingTime duration for which a client is blocked after exceeding error threshold
     * @param errorAcceptanceWindow time window for counting errors per client
     */
    public RateLimitedHandlerList(
            int maxClientsToTrack,
                long maxGlobalRequestsPerSecond,
                                  int maxErrorsPerClient,
                                  int perClientPercent,
                                  Duration clientIdleTime,
                                  Duration clientBlockingTime,
                                  Duration errorAcceptanceWindow) {

        perClientRates = CacheBuilder.newBuilder()
                .initialCapacity(CLIENT_IP_CACHE_INITIAL_CAPACITY)
                .maximumSize(maxClientsToTrack)
                .expireAfterAccess(clientIdleTime)
                .build();

        blockedClients = CacheBuilder.newBuilder()
                .maximumSize(maxClientsToTrack)
                .expireAfterWrite(clientBlockingTime)
                .build();

        perClientErrorCount = CacheBuilder.newBuilder()
                .initialCapacity(CLIENT_IP_CACHE_INITIAL_CAPACITY)
                .maximumSize(maxClientsToTrack)
                .expireAfterAccess(errorAcceptanceWindow)
                .build();

        globalRateLimiter = RateLimiter.create(maxGlobalRequestsPerSecond);
        perClientRate = perClientPercent * maxGlobalRequestsPerSecond / 100.0d;
        this.maxErrorsPerClient = maxErrorsPerClient;
    }


    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

        String client = getClientIp(request);

        boolean blocked = blockedClients.getIfPresent(client) != null;
        if (blocked) {
            LOGGER.debug("Blocking client with too many auth errors {}", client);
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS_429);
            response.getWriter().write("Server is busy. Please try again later.");
            baseRequest.setHandled(true);
            return;
        }

        if (!getClientRateLimiter(client).tryAcquire()) {
            LOGGER.debug("Blocking client with too many requests {}", client);
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS_429);
            response.getWriter().write("Server is busy. Please try again later.");
            baseRequest.setHandled(true);
            return;
        }

        if (!globalRateLimiter.tryAcquire()) {
            LOGGER.debug("Blocking client due to globally too many requests {}", client);
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS_429);
            response.getWriter().write("Server is busy. Please try again later.");
            baseRequest.setHandled(true);
            return;
        }

        Handler[] handlers = this.getHandlers();
        if (handlers != null && this.isStarted()) {
            for (Handler handler : handlers) {
                handler.handle(target, baseRequest, request, response);
                if (baseRequest.isHandled()) {
                    // block clients that hammer with authentication failures
                    if (response.getStatus() == 401) {
                        int errors = getClientErrorRateLimiter(client).incrementAndGet();
                        if (errors >= maxErrorsPerClient) {
                            LOGGER.warn("Blocking client due to too many auth errors: {}", client);
                            blockedClients.put(client, BLOCK);
                            // as client blocked, no reason to keep track of further errors
                            perClientErrorCount.invalidate(client);
                            perClientRates.invalidate(client);
                        }
                    }
                    return;
                }
            }
        }
    }

    /**
     * Extracts the client IP address from the request, considering possible proxies.
     *
     * @param request the HTTP request
     * @return the client IP address
     */
    private String getClientIp(HttpServletRequest request) {
        String forwardedIp = request.getHeader("X-Forwarded-For");
        if (forwardedIp == null) {
            return request.getRemoteAddr();
        }
        return forwardedIp.split(",")[0];
    }

    /**
     * Retrieves or creates a RateLimiter for the specified client.
     *
     * @param client the client identifier (e.g., IP address)
     * @return the RateLimiter for the client
     */
    private RateLimiter getClientRateLimiter(String client) {
        try {
            return perClientRates.get(client, () -> RateLimiter.create(perClientRate));
        } catch (Exception e) {
            // should not happen
            throw new RuntimeException("Failed to get or create rate limiter for client " + client, e);
        }
    }

    /**
     * Retrieves or creates an AtomicInteger to count errors for the specified client.
     *
     * @param client the client identifier (e.g., IP address)
     * @return the AtomicInteger counting errors for the client
     */
    private AtomicInteger getClientErrorRateLimiter(String client) {
        try {
            return perClientErrorCount.get(client, () -> new AtomicInteger(0));
        } catch (Exception e) {
            // should not happen
            throw new RuntimeException("Failed to get or create error counter per client client " + client, e);
        }
    }

    @Override
    public String toString() {
        return String.format("RateLimitedHandlerList{globalRate=%.1f, perClientRate=%.1f, maxErrorsPerClient=%d}",
                globalRateLimiter.getRate(), perClientRate, maxErrorsPerClient);
    }

    @VisibleForTesting
    void setMaxGlobalRequestsPerSecond(int maxRequestsPerSecond) {
        checkArgument(maxRequestsPerSecond > 0, "maxRequestsPerSecond must be positive");
        globalRateLimiter.setRate(maxRequestsPerSecond);
    }

    @Command(name="limits reset", description="Reset all rate limiters and error counters")
    public class LimitsResetCommand implements Callable<String> {
        @Override
        public String call() {
            perClientRates.invalidateAll();
            blockedClients.invalidateAll();
            perClientErrorCount.invalidateAll();

            return "";
        }
    }


    @Command(name="limits info", description="Show current rate limits and statistics. The retuned information is approximate.")
    public class LimitsShowCommand implements Callable<String> {

        @Option(name="l", usage="Verbose listing")
        boolean verbose = false;

        @Override
        public String call() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Global rate: %.1f requests/second\n", globalRateLimiter.getRate()));
            sb.append(String.format("Per-client rate: %.1f requests/second\n", perClientRate));
            sb.append(String.format("Max errors per client before blocking: %d\n", maxErrorsPerClient));
            sb.append(String.format("Currently blocked clients: ~%d\n", blockedClients.size()));
            if (verbose) {
                sb.append("  Blocked clients:\n");
                blockedClients.asMap().keySet().forEach(client -> sb.append("    ").append(client).append("\n"));
            }
            sb.append(String.format("Tracked clients with rate limiters: ~%d\n", perClientRates.size()));
            if (verbose) {
                sb.append("  Clients with rate limiters:\n");
                perClientRates.asMap().keySet().forEach(client -> sb.append("    ").append(client).append("\n"));
            }
            sb.append(String.format("Tracked clients with error counters: ~%d\n", perClientErrorCount.size()));
            if (verbose) {
                sb.append("  Clients with error counters:\n");
                perClientErrorCount.asMap().forEach((client, counter) ->
                        sb.append("    ").append(client).append(": ").append(counter.get()).append(" errors\n"));
            }
            return sb.toString();
        }
    }
}
