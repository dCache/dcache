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
package org.dcache.restful.events.streams;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.glassfish.jersey.internal.guava.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.dcache.auth.Subjects;
import org.dcache.restful.events.spi.EventStream;
import org.dcache.restful.events.spi.SelectedEventStream;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.restful.util.transfers.Json.readFromJar;

import org.dcache.restful.events.spi.SelectionResult;
import org.dcache.restful.events.spi.SelectionStatus;
import org.dcache.restful.events.streams.Metronome.MetronomeSelector;
import org.dcache.restful.util.RequestUser;

import static org.dcache.restful.events.spi.SelectionResult.badSelector;

/**
 * Emits a stream of events with a fixed period.  This provides both example
 * code to illustrate how to write a simple event stream, and a source of
 * events that clients can test against.
 */
public class Metronome implements EventStream
{
    @JsonInclude(Include.NON_NULL)
    public static class MetronomeSelector
    {
        private long frequency = -1; // value is in Hz
        private String suppliedMessage;
        private String message = DEFAULT_MESSAGE;
        private Long suppliedCount;
        private long remaining = -1;

        public void setFrequency(long value)
        {
            frequency = value;
        }

        public long getFrequency()
        {
            return frequency;
        }

        public void setMessage(String value)
        {
            suppliedMessage = value;
            String username = Subjects.getUserName(RequestUser.getSubject());
            message = value.replace("${username}", username);
        }

        public String getMessage()
        {
            return suppliedMessage;
        }

        public void setCount(long value)
        {
            suppliedCount = value;
            remaining = value;
        }

        public Long getCount()
        {
            return suppliedCount;
        }

        public JsonNode nextEvent()
        {
            if (remaining == 0) {
                return EventStream.CLOSE_STREAM;
            }

            if (remaining > 0) {
                remaining--;
            }
            return JsonNodeFactory.instance.textNode(message);
        }

        public long getPeriod(TimeUnit unit)
        {
            return Math.round((double)unit.convert(1, SECONDS) / frequency);
        }

        public SelectionResult validationError()
        {
            if (frequency == 0) {
                return badSelector("\"frequency\" must be specified");
            }
            if (frequency < MINIMUM_HZ) {
                return badSelector("\"frequency\" is too small (less than %d)", MINIMUM_HZ);
            }
            if (frequency > MAXIMUM_HZ) {
                return badSelector("\"frequency\" is too big (more than %d)", MAXIMUM_HZ);
            }
            if (message.isEmpty()) {
                return badSelector("\"message\" cannot be empty");
            }
            if (remaining != -1 && remaining <= 0) {
                return badSelector("\"count\" must be greater than zero");
            }

            return null;
        }
    }

    private class MetronomeSelection implements SelectedEventStream
    {
        private final BiConsumer<String,JsonNode> receiver;
        private final String id = UUID.randomUUID().toString();
        private final MetronomeSelector selector;
        private ScheduledFuture ticker;
        private AtomicInteger count = new AtomicInteger();

        public MetronomeSelection(BiConsumer<String,JsonNode> receiver, MetronomeSelector selector)
        {
            this.receiver = (s,n) -> {
                        count.incrementAndGet();
                        receiver.accept(s, n);
                    };
            this.selector = selector;
            startTicking(selector);
        }

        @Override
        public JsonNode selector()
        {
            return new ObjectMapper().convertValue(selector, JsonNode.class);
        }

        @Override
        public String getId()
        {
            return id;
        }

        private void startTicking(MetronomeSelector selector)
        {
            ticker = service.scheduleAtFixedRate(() -> receiver.accept(id, selector.nextEvent()),
                    0, selector.getPeriod(MICROSECONDS), MICROSECONDS);
        }


        @Override
        public synchronized void close()
        {
            ticker.cancel(false);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("id", id)
                    .add("selector", selector)
                    .add("active", !ticker.isDone())
                    .add("events", count)
                    .toString();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Metronome.class);
    private static final String DEFAULT_MESSAGE = "tick";
    private static final int MINIMUM_HZ = 1;
    private static final int MAXIMUM_HZ = 1_000_000;
    private static final ObjectNode SELECTORS_SCHEMA =
            withConstants(readFromJar("/org/dcache/frontend/metronome/selectors-schema.json"));
    private static final ObjectNode EVENTS_SCHEMA = readFromJar("/org/dcache/frontend/metronome/events-schema.json");

    private final Map<String,MetronomeSelection> selectionById = new HashMap<>();
    private final ScheduledExecutorService service = new ScheduledThreadPoolExecutor(0,
            new ThreadFactoryBuilder().setNameFormat("metronome-%d").build());

    private static ObjectNode withConstants(ObjectNode object)
    {
        ObjectNode properties = (ObjectNode) object.get("properties");
        ObjectNode frequency = (ObjectNode) properties.get("frequency");
        frequency.put("minimum", MINIMUM_HZ);
        frequency.put("maximum", MAXIMUM_HZ);

        ObjectNode message = (ObjectNode) properties.get("message");
        message.put("default", DEFAULT_MESSAGE);
        return object;
    }

    @Override
    public ObjectNode selectorSchema()
    {
        return SELECTORS_SCHEMA;
    }

    @Override
    public ObjectNode eventSchema()
    {
        return EVENTS_SCHEMA;
    }

    @Override
    public SelectionResult select(String channelId, BiConsumer<String,JsonNode> receiver, JsonNode serialisedSelector)
    {
        MetronomeSelector selector = deserialise(serialisedSelector);
        SelectionResult result = selector.validationError();
        if (result != null) {
            return result;
        }
        MetronomeSelection ses = new MetronomeSelection(receiver, selector);
        selectionById.put(ses.getId(), ses);
        LOGGER.debug("Created new selection {}", ses);
        return SelectionResult.created(ses);
    }

    public void shutdown()
    {
        service.shutdown();
    }

    @Override
    public String eventType()
    {
        return "metronome";
    }

    @Override
    public String description()
    {
        return "a regular stream of events at the configured rate";
    }

    private MetronomeSelector deserialise(JsonNode serialised)
    {
        try {
            return new ObjectMapper().readerFor(MetronomeSelector.class).readValue(serialised);
        } catch (JsonMappingException e) {
            int index = e.getMessage().indexOf('\n');
            String msg = index == -1 ? e.getMessage() : e.getMessage().substring(0, index);
            throw new BadRequestException("Bad selector value: " + msg);
        } catch (IOException e) {
            throw new InternalServerErrorException("Unable to process selector: " + e.getMessage());
        }
    }
}
