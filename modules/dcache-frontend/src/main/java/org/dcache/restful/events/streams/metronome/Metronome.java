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
package org.dcache.restful.events.streams.metronome;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.dcache.restful.events.spi.EventStream;
import org.dcache.restful.events.spi.SelectionContext;

import static org.dcache.restful.util.transfers.Json.readFromJar;

import org.dcache.restful.events.spi.SelectionResult;

/**
 * A plugin that emits a configurable stream of string messages.  The time
 * between successive messages may be specified as a frequency (in Hz) or as
 * a delay (in seconds).  It is also possible to introduce some randomness in
 * the delay.
 * <p>
 * The message may be specified.  Any occurances of {@literal ${user}} are
 * replaced by the requesting user's username.  Any occurances of
 * {@literal ${count}} are replaced by the message number.
 * <p>
 * This plugin provides both example code to illustrate how to write an
 * EventStream and an example source of events so that users writing a client
 * have sample events to test their client against.
 */
public class Metronome implements EventStream
{
    public static final String DEFAULT_MESSAGE = "tick";
    public static final double MAXIMUM_HZ = 1_000_000;
    public static final double MAXIMUM_DELAY = TimeUnit.MINUTES.toSeconds(5);
    public static final double MINIMUM_HZ = 1/MAXIMUM_DELAY;
    public static final double MINIMUM_DELAY = 1/MAXIMUM_HZ;

    private static final Logger LOGGER = LoggerFactory.getLogger(Metronome.class);
    private static final ObjectNode SELECTORS_SCHEMA =
            withConstants(readFromJar("/org/dcache/frontend/metronome/selectors-schema.json"));
    private static final ObjectNode EVENTS_SCHEMA = readFromJar("/org/dcache/frontend/metronome/events-schema.json");

    private final Map<String,Selection> selectionById = new HashMap<>();
    private final ScheduledExecutorService service = new ScheduledThreadPoolExecutor(0,
            new ThreadFactoryBuilder().setNameFormat("metronome-%d").build());

    private static ObjectNode withConstants(ObjectNode object)
    {
        ObjectNode properties = (ObjectNode) object.get("properties");

        ObjectNode frequency = (ObjectNode) properties.get("frequency");
        frequency.put("minimum", MINIMUM_HZ);
        frequency.put("maximum", MAXIMUM_HZ);

        ObjectNode delay = (ObjectNode) properties.get("delay");
        delay.put("minimum", MINIMUM_DELAY);
        delay.put("maximum", MAXIMUM_DELAY);

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
    public SelectionResult select(SelectionContext context,
            BiConsumer<String,JsonNode> receiver, JsonNode serialisedSelector)
    {
        Selector selector = deserialise(serialisedSelector);
        SelectionResult result = selector.validationError();
        if (result != null) {
            return result;
        }
        Selection selection = new Selection(receiver, service, selector);
        selectionById.put(selection.getId(), selection);
        LOGGER.debug("Created new selection {}", selection);
        return SelectionResult.created(selection);
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
        return "a configurable stream of messages";
    }

    private Selector deserialise(JsonNode serialised)
    {
        try {
            return new ObjectMapper().readerFor(Selector.class).readValue(serialised);
        } catch (JsonMappingException e) {
            int index = e.getMessage().indexOf('\n');
            String msg = index == -1 ? e.getMessage() : e.getMessage().substring(0, index);
            throw new BadRequestException("Bad selector value: " + msg);
        } catch (IOException e) {
            throw new InternalServerErrorException("Unable to process selector: "
                    + e.getMessage());
        }
    }
}
