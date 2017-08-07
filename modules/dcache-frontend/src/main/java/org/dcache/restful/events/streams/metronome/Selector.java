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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.MoreObjects;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.dcache.auth.Subjects;
import org.dcache.restful.events.spi.EventStream;
import org.dcache.restful.events.spi.SelectionResult;
import org.dcache.restful.util.RequestUser;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.restful.events.spi.SelectionResult.badSelector;

/**
 * The selector that configures a stream of messages.  This holds the
 * information the client supplied when requesting a stream of messages.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Selector
{
    private Double frequency; // value is in Hz
    private Double delay; // value is in seconds
    private String suppliedMessage;
    private String message = Metronome.DEFAULT_MESSAGE;
    private long suppliedCount;
    private long count;
    private boolean hasMessageCount;

    public void setFrequency(double value)
    {
        frequency = value;
    }

    public Double getFrequency()
    {
        return frequency;
    }

    public void setDelay(double value)
    {
        delay = value;
    }

    public Double getDelay()
    {
        return delay;
    }

    public void setMessage(String value)
    {
        suppliedMessage = value;
        String username = Subjects.getUserName(RequestUser.getSubject());
        message = value.replace("${username}", username);
        hasMessageCount = message.contains("${count}");
    }

    public String getMessage()
    {
        return suppliedMessage;
    }

    public void setCount(long value)
    {
        suppliedCount = value;
    }

    public Long getCount()
    {
        return suppliedCount == 0 ? null : suppliedCount;
    }

    public void sendEvents(Consumer<JsonNode> receiver)
    {
        count++;
        String thisMessage = hasMessageCount ?
                message.replace("${count}", Long.toString(count)) : message;
        receiver.accept(JsonNodeFactory.instance.textNode(thisMessage));

        if (suppliedCount > 0 && count >= suppliedCount) {
            receiver.accept(EventStream.CLOSE_STREAM);
        }
    }

    public long tickDelay(TimeUnit unit)
    {
        checkArgument(SECONDS.compareTo(unit) >= 0,
                "requested TimeUnit must be SECONDS or shorter");
        double nextDelay = frequency == null ? delay : (1 / frequency);
        return Math.round(unit.convert(1L, SECONDS) * nextDelay);
    }

    public SelectionResult validationError()
    {
        if (frequency == null && delay == null) {
            return badSelector("either \"frequency\" or \"delay\" must be specified");
        }
        if (frequency != null && delay != null) {
            return badSelector("\"frequency\" and \"delay\" cannot both be specified");
        }
        if (frequency != null) {
            if (frequency < Metronome.MINIMUM_HZ) {
                return badSelector("\"frequency\" is too small (less than %g)",
                        Metronome.MINIMUM_HZ);
            }
            if (frequency > Metronome.MAXIMUM_HZ) {
                return badSelector("\"frequency\" is too big (more than %g)",
                        Metronome.MAXIMUM_HZ);
            }
        }
        if (delay != null) {
            if (delay < Metronome.MINIMUM_DELAY) {
                return badSelector("\"delay\" is too small (less than %g)",
                        Metronome.MINIMUM_DELAY);
            }
            if (delay > Metronome.MAXIMUM_DELAY) {
                return badSelector("\"delay\" is too big (more than %g)",
                        Metronome.MAXIMUM_DELAY);
            }
        }
        if (message.isEmpty()) {
            return badSelector("\"message\" cannot be empty");
        }
        if (suppliedCount < 0) {
            return badSelector("\"count\" must be non-negative");
        }

        return null;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("message", suppliedMessage)
                .add("frequency", frequency)
                .add("delay", delay)
                .add("count", suppliedCount)
                .toString();
    }
}
