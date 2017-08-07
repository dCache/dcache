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
package org.dcache.restful.events.spi;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Represents a client's subscription to some subset of an EventStream's
 * events.
 */
public interface SelectedEventStream
{
    /**
     * Provide the ID associated with this selection.
     */
    String getId();

    /**
     * Provide the selector that is currently in effect.
     */
    JsonNode selector();

    /**
     * Stop sending events.  This method is called either when the client
     * requests to cancel the subscription, or when the channel is closed.
     * Calling this method multiple times has the same effect as calling it
     * once.
     */
    void close();
}
