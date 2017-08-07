/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.EvictingQueue;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.sse.OutboundSseEvent;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;

import java.io.EOFException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.dcache.auth.Subjects;
import org.dcache.restful.events.spi.EventStream;
import org.dcache.restful.events.spi.SelectionResult;
import org.dcache.restful.events.spi.SelectionStatus;
import org.dcache.restful.util.CloseableWithTasks;

import static com.google.common.base.Preconditions.checkState;

/**
 * A channel represents a client's desire to know about events.  Each HTTP
 * client should have a single channel object, which has a corresponding unique
 * URL.
 * <p>
 * The channel (and corresponding URL) allows the client to receive events via
 * an HTTP GET request, as described in the SSE protocol.  In addition the URL
 * also forms the basis for management of events.
 * <p>
 * It is expected that a channel object is not shared between multiple clients
 * and that each client has (at most) a single ongoing SSE request (i.e., a
 * HTTP GET request through which the client will receive events).  Any incoming
 * SSE request will trigger the termination of any existing SSE request for
 * this channel.
 * <p>
 * The SSE protocol allows reconnecting clients to indicate the ID of the last
 * event they processed.  In theory, this allows dCache to send any events
 * that the client missed.  However, the SSE protocol provides no feedback on
 * which events have been consumed successfully.  Therefore, we should keep all
 * events until the client provides such feedback by reconnecting.
 * <p>
 * As a compromise, previous events are kept in a fixed-size ring buffer.  If
 * the client reconnects "fast enough" then dCache is able to send all events
 * that were missed.
 */
public class Channel extends CloseableWithTasks
{
    /**
     * Represents an individual event being sent to a specific client.  A
     * single internal event (as delivered to its receiver by some EventStream)
     * may generate multiple Event objects: one object for each client that has
     * subscribed to the event.
     */
    private class Event
    {
        private final int id;
        private final String type;
        private final String data;

        public Event(int id, String type, String data)
        {
            this.id = id;
            this.type = type;
            this.data = data;
        }

        /**
         * @return false if the Sink was detected as closed before sending
         * the event, true otherwise.
         */
        public boolean sendEvent()
        {
            if (sse == null || sink == null) {
                return false;
            }

            OutboundSseEvent event = sse.newEventBuilder()
                    .data(data)
                    .name(type)
                    .id(Integer.toString(id))
                    .build();
            return Channel.this.sendEvent(event);
        }

        @Override
        public int hashCode()
        {
            return id;
        }

        @Override
        public boolean equals(Object other)
        {
            return other == this ||
                    (other instanceof Event) && ((Event)other).id == id;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Channel.class);

    private final EvictingQueue<Event> ringBuffer = EvictingQueue.create(16384);
    private final ScheduledExecutorService executor;
    private final AtomicInteger queueSize = new AtomicInteger();
    private final Map<SubscriptionId,Subscription> subscriptionsByIdentity
            = new ConcurrentHashMap<>();

    private final long owner;
    private final EventStreamRepository repository;
    private final BiFunction<String,String,String> subscriptionValueBuilder;

    // Stopwatch is always running after the first event has been sent.
    private final Stopwatch lastSentEvent = Stopwatch.createUnstarted();

    private SseEventSink sink;
    private Sse sse;
    private ScheduledFuture closeFuture;
    private long timeout;
    private int nextEventId = 0;

    public Channel(ScheduledExecutorService executor,
            EventStreamRepository repository, Subject subject, long timeout,
            BiFunction<String,String,String> subscriptionValueBuilder)
    {
        this.executor = executor;
        this.repository = repository;
        this.owner = Subjects.getUid(subject);

        this.timeout = timeout;
        this.subscriptionValueBuilder = subscriptionValueBuilder;

        onClose(() -> {
                    subscriptionsByIdentity.values().forEach(Subscription::close);
                    if (sink != null && !sink.isClosed()) {
                        sink.close();
                        sink = null;
                        sse = null;
                    }
                    ringBuffer.clear();
                    if (closeFuture != null) {
                        closeFuture.cancel(false); // do not interrupt ourself
                        closeFuture = null;
                    }
                });

        // Time limit for client to connect after creating a channel
        scheduleClose();
    }

    public boolean isAccessAllowed(Subject requestor)
    {
        return Subjects.getUid(requestor) == owner;
    }

    public synchronized void sendEvent(String eventType, String selectionId, JsonNode eventPayload)
    {
        if (eventPayload == EventStream.CLOSE_STREAM) {
            getSubscription(eventType, selectionId).ifPresent(Subscription::close);
            return;
        }

        if (isClosed()) {
            return;
        }

        int eventId = nextEventId++;

        ObjectNode eventEnvelope = JsonNodeFactory.instance.objectNode();
        eventEnvelope.set("event", eventPayload);
        eventEnvelope.put("subscription", subscriptionValueBuilder.apply(eventType, selectionId));

        try {
            String eventData = new ObjectMapper().writeValueAsString(eventEnvelope);
            Event event = new Event(eventId, eventType, eventData);
            ringBuffer.add(event);
            event.sendEvent();
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialise JSON: {}", e.getMessage());
        }
    }

    /**
     * @return false if the Sink was detected as closed before sending the
     * event, true otherwise.
     */
    @GuardedBy("this")
    private boolean sendEvent(OutboundSseEvent event)
    {
        if (sink.isClosed()) {
            sinkClosed();
            return false;
        }

        int newSize = queueSize.incrementAndGet();
        if (newSize > 1) {
            LOGGER.info("Queue size now {}", newSize);
            // REVISIT should we kick out the client if the client too slow?
        }
        sink.send(event).handle((o, t) -> {
                    queueSize.decrementAndGet();

                    // Jersey provides connection errors as normal result.
                    if (o instanceof Throwable) {
                        t = (Throwable) o;
                        o = null;
                    }

                    if (t instanceof ClosedChannelException || t instanceof EOFException) {
                        sinkClosed();
                    } else if (t instanceof RuntimeException) {
                        LOGGER.error("Bug detected: please report this to <support@dcache.org>", t);
                    } else if (t instanceof Exception) {
                        LOGGER.error("Failed to send event: {}", t.toString());
                    } else if (t instanceof Error) {
                        throw (Error) t;
                    } else if (o != null) {
                        LOGGER.warn("Send resulted in {}", o);
                    }

                    return new CompletableFuture();
                });
        lastSentEvent.reset().start();
        return true;
    }

    @GuardedBy("this")
    private void sinkClosed()
    {
        if (sink != null) {
            sink = null;
            sse = null;
            LOGGER.debug("Client has disconnected");
            scheduleClose();
        }
    }

    @GuardedBy("this")
    private void scheduleClose()
    {
        closeFuture = executor.schedule(this::close, timeout, TimeUnit.MILLISECONDS);
    }

    public synchronized void sendKeepAlive()
    {
        if (!isClosed() && sse != null && sink != null
                && (!lastSentEvent.isRunning() || lastSentEvent.elapsed(TimeUnit.SECONDS) > 5)) {
            sendEvent(sse.newEventBuilder().comment("").build());
        }
    }

    public synchronized void acceptConnection(Sse newSse, SseEventSink newSink, String lastId)
    {
        checkState(!isClosed());

        if (sink != null && !sink.isClosed()) {
            // Permit only one SSE (HTTP GET) operation per channel.
            sink.close();
        }

        if (closeFuture != null) {
            if (closeFuture.isDone()) {
                throw new NotFoundException("Channel is closed"); // This shouldn't really happen.
            }
            closeFuture.cancel(true);
            closeFuture = null;
        }

        sink = newSink;
        sse = newSse;

        Integer id = Ints.tryParse(Strings.nullToEmpty(lastId));
        if (id != null) {
            Event clientSuppliedLastEvent = new Event(id, null, null);

            // Keep the last sent event in the buffer, so that subsequent
            // requests with the same Last-Event-Id value can return the same output.
            while (!ringBuffer.isEmpty()) {
                if (clientSuppliedLastEvent.equals(ringBuffer.peek())) {
                    break;
                }
                ringBuffer.poll();
            }

            if (ringBuffer.isEmpty()) {
                sendEvent(sse.newEventBuilder()
                        .name("SYSTEM")
                        .data("{\"type\":\"event-loss\"}")
                        .build());
            } else {
                ringBuffer.stream()
                        .skip(1)
                        .forEach(Event::sendEvent);
            }
        }
    }

    public long getTimeout()
    {
        return timeout;
    }

    public synchronized void updateTimeout(long newTimeout)
    {
        timeout = newTimeout;
        rescheduleClose();
    }

    public synchronized void rescheduleClose()
    {
        if (!isClosed() && closeFuture != null) {
            closeFuture.cancel(true);
            scheduleClose();
        }
    }

    public SubscriptionResult subscribe(String channelId, String eventType, JsonNode selector)
    {
        EventStream es = repository.getEventStream(eventType)
                    .orElseThrow(() -> new BadRequestException("Unknown event type: " + eventType));

        SelectionResult result = es.select(channelId,
                (id,event) -> {
                            try {
                                sendEvent(eventType, id, event);
                            } catch (RuntimeException e) {
                                LOGGER.error("Bug found in dCache, please report to <support@dCache.org>", e);
                            }
                        }, selector);

        if (result.getStatus() == SelectionStatus.CREATED) {
            SubscriptionId id = new SubscriptionId(eventType, result.getSelectedEventStream().getId());
            Subscription s = new Subscription(eventType, result.getSelectedEventStream());
            subscriptionsByIdentity.put(id, s);
            s.onClose(() -> subscriptionsByIdentity.remove(id, s));
        }

        return new SubscriptionResult(result);
    }

    public List<Subscription> getSubscriptions()
    {
        return new ArrayList<>(subscriptionsByIdentity.values());
    }

    public Optional<Subscription> getSubscription(String eventType, String subscriptionId)
    {
        SubscriptionId id = new SubscriptionId(eventType, subscriptionId);
        return Optional.ofNullable(subscriptionsByIdentity.get(id));
    }
}
