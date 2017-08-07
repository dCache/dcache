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

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.ApiModelProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.dcache.restful.events.spi.EventStream;

/**
 * The collection of known EventStream instances.
 */
public class EventStreamRepository
{
    public class EventStreamMetadata
    {
        @ApiModelProperty("Provide a short (typically single sentence) "
                + "description of the generated events.")
        public final String description;

        public EventStreamMetadata(EventStream stream)
        {
            description = stream.description();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamRepository.class);

    private final Map<String,EventStream> streams = new HashMap<>();

    @Required
    public void setPlugins(List<EventStream> streams)
    {
        LOGGER.debug("Received plugins: {}", streams);

        for (EventStream stream : streams) {
            EventStream previous = this.streams.put(stream.eventType(), stream);
            if (previous != null) {
                throw new IllegalArgumentException("Duplicate event type "
                        + stream.eventType() + ": " + stream.getClass() + " and "
                        + previous.getClass());
            }
        }
    }

    public Optional<EventStream> getEventStream(String eventType)
    {
        return Optional.ofNullable(streams.get(eventType));
    }

    public List<String> listEventTypes()
    {
        return streams.keySet().stream()
                .sorted()
                .collect(Collectors.toList());
    }

    public Optional<EventStreamMetadata> metadataForEventType(String type)
    {
        return Optional.ofNullable(streams.get(type))
                    .map(EventStreamMetadata::new);
    }

    public Optional<ObjectNode> selectorSchemaForEventType(String type)
    {
        return Optional.ofNullable(streams.get(type))
                    .map(EventStream::selectorSchema);
    }

    public Optional<ObjectNode> eventSchemaForEventType(String type)
    {
        return Optional.ofNullable(streams.get(type))
                    .map(EventStream::eventSchema);
    }
}
