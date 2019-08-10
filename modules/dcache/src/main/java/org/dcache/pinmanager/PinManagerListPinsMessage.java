/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.pinmanager;

import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

import org.dcache.pinmanager.model.Pin;

/**
 * A message that requests information about all pins targeting a specific
 * file.
 */
public class PinManagerListPinsMessage extends Message
{
    public enum State
    {
        PINNING, PINNED, UNPINNING
    }

    public static class Info implements Serializable
    {
        private static final long serialVersionUID = 1L;
        private final Instant creationTime;
        private final Instant expirationTime;
        private final State state;
        private final String requestId;
        private final boolean canUnpin;
        private final long id;

        public Info(Pin pin, boolean canUnpin)
        {
            creationTime = pin.getCreationTime().toInstant();
            Date expiration = pin.getExpirationTime();
            expirationTime = expiration == null ? null : expiration.toInstant();
            state = TO_MESSAGE_STATE.get(pin.getState());
            requestId = pin.getRequestId();
            id = pin.getPinId();
            this.canUnpin = canUnpin;
        }

        public Instant getCreationTime()
        {
            return creationTime;
        }

        public Optional<Instant> getExpirationTime()
        {
            return Optional.ofNullable(expirationTime);
        }

        public State getState()
        {
            return state;
        }

        public Optional<String> getRequestId()
        {
            return Optional.ofNullable(requestId);
        }

        public long getId()
        {
            return id;
        }

        public boolean isUnpinnable()
        {
            return canUnpin;
        }
    }

    private static final ImmutableMap<Pin.State,State> TO_MESSAGE_STATE = ImmutableMap.of(
            Pin.State.PINNING, State.PINNING,
            Pin.State.PINNED, State.PINNED,
            Pin.State.UNPINNING, State.UNPINNING);
    private static final long serialVersionUID = 1L;
    private final PnfsId _pnfsId;
    private List<Info> _info;

    public PinManagerListPinsMessage(PnfsId id)
    {
        _pnfsId = id;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public void setInfo(List<Info> info)
    {
        _info = info;
    }

    public List<Info> getInfo()
    {
        return _info;
    }
}
