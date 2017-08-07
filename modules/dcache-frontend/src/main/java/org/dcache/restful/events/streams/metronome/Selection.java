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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;

import org.dcache.restful.events.spi.SelectedEventStream;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Represents the selected stream of events.  It contains the client-supplied
 * JSON selector along with other, operational elements.
 */
class Selection implements SelectedEventStream
{
    private final BiConsumer<String,JsonNode> receiver;
    private final String id = UUID.randomUUID().toString();
    private final Selector selector;
    private ScheduledFuture ticking;

    public Selection(BiConsumer<String,JsonNode> receiver,
            ScheduledExecutorService service, Selector selector)
    {
        this.receiver = receiver;
        this.selector = selector;
        ticking = startTicking(service, selector);
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

    private ScheduledFuture startTicking(ScheduledExecutorService service, Selector selector)
    {
        long delay = selector.tickDelay(MICROSECONDS);
        return service.scheduleAtFixedRate(() -> selector.sendEvents(d -> receiver.accept(id, d)),
                delay, delay, MICROSECONDS);
    }

    @Override
    public synchronized void close()
    {
        if (ticking != null) {
            ticking.cancel(false);
            ticking = null;
        }
    }

    @Override
    public String toString()
    {
        boolean isActive;
        synchronized (this) {
            isActive = ticking != null;
        }
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("selector", selector)
                .add("active", isActive)
                .toString();
    }
}
