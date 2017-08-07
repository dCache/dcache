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

import com.google.common.io.BaseEncoding;
import org.springframework.beans.factory.annotation.Required;

import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.ws.rs.ClientErrorException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.dcache.auth.Subjects;

/**
 *  The Registrar is responsible for holding client channels.
 */
public class Registrar
{
    @FunctionalInterface
    public interface SubscriptionValueBuilder
    {
        String buildUrl(String channelId, String eventType, String subscriptionId);
    }

    /**
     * An identifier that combines the UID of a user and the client-id.
     */
    private static class UidClientId
    {
        private final long uid;
        private final String clientId;

        UidClientId(String clientId, long uid)
        {
            this.clientId = clientId;
            this.uid = uid;
        }

        @Override
        public int hashCode()
        {
            return (int)uid ^ (clientId == null ? 0 : clientId.hashCode());
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this) {
                return true;
            }

            if (!(other instanceof UidClientId)) {
                return false;
            }

            UidClientId otherId = (UidClientId)other;
            return otherId.uid == uid && Objects.equals(otherId.clientId, clientId);
        }
    }

    private final Map<String,Channel> _channels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long,List<String>> _channelsByUid = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UidClientId,List<String>> _channelsByUidClientId = new ConcurrentHashMap<>();
    private final Random random = new Random();
    private int maximumChannelsPerUser;
    private long defaultDisconnectTimeout;
    private volatile ScheduledFuture keepAlive;

    @Inject
    private ScheduledExecutorService executor;

    @Inject
    private EventStreamRepository repository;

    @Required
    public void setMaximumChannelsPerUser(int max)
    {
        maximumChannelsPerUser = max;
    }

    public int getMaximumChannelsPerUser()
    {
        return maximumChannelsPerUser;
    }

    @Required
    public void setDefaultDisconnectTimeout(long timeout)
    {
        defaultDisconnectTimeout = timeout;
    }

    public long getDefaultDisconnectTimeout()
    {
        return defaultDisconnectTimeout;
    }

    public String newChannel(Subject user, String clientId, SubscriptionValueBuilder serialiser)
    {
        byte[] r = new byte[16]; // 128 bit is equivalent to a UUID.
        random.nextBytes(r);
        String id = BaseEncoding.base64Url().omitPadding().encode(r);

        long uid = Subjects.getUid(user);

        List<String> existingIds = _channelsByUid.computeIfAbsent(uid,
                (u) -> new ArrayList());
        synchronized (existingIds) {
            if (existingIds.size() >= maximumChannelsPerUser) {
                throw new ClientErrorException("Too Many Channels", 429);
            }
            existingIds.add(id);
        }

        UidClientId uci = new UidClientId(clientId, uid);
        List<String> existingClientIds = _channelsByUidClientId.computeIfAbsent(uci,
                (u) -> new ArrayList());
        synchronized (existingClientIds) {
            existingClientIds.add(id);
        }

        Channel channel = new Channel(executor, repository, user, defaultDisconnectTimeout,
                (type, subId) -> serialiser.buildUrl(id, type, subId));
        _channels.put(id, channel);

        channel.onClose(() -> {
                    _channels.remove(id);
                    synchronized (existingIds) {
                        existingIds.remove(id);
                    }
                    synchronized (existingClientIds) {
                        existingClientIds.remove(id);
                    }
                });
        return id;
    }

    public Optional<Channel> get(String id)
    {
        return Optional.ofNullable(_channels.get(id));
    }

    public List<String> idsForUser(Subject subject)
    {
        List<String> ids = _channelsByUid.get(Subjects.getUid(subject));
        if (ids == null) {
            return Collections.emptyList();
        }
        synchronized (ids) {
            return new ArrayList<>(ids);
        }
    }

    public List<String> idsForUser(Subject subject, String clientId)
    {
        UidClientId uci = new UidClientId(clientId, Subjects.getUid(subject));

        List<String> ids = _channelsByUidClientId.get(uci);
        if (ids == null) {
            return Collections.emptyList();
        }
        synchronized (ids) {
            return new ArrayList<>(ids);
        }
    }

    public void start()
    {
        keepAlive = executor.scheduleWithFixedDelay(
                () -> _channels.values().forEach(Channel::sendKeepAlive),
                10, 10, TimeUnit.SECONDS);
    }

    public void stop()
    {
        keepAlive.cancel(false);
    }
}
