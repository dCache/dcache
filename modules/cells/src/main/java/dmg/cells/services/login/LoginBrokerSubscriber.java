/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package dmg.cells.services.login;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.command.Command;
import dmg.util.command.Option;

import static com.google.common.collect.Collections2.*;
import static dmg.cells.services.login.LoginBrokerInfo.Capability.READ;
import static dmg.cells.services.login.LoginBrokerInfo.Capability.WRITE;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Subscriber of LoginBrokerInfo updates.
 *
 * Maintains a list of available doors. A door is removed from the list when
 * an update hasn't been received for 2.5 times the update time of that door.
 * This allows one missed update before the door is removed.
 */
public class LoginBrokerSubscriber
        implements CellCommandListener, CellMessageReceiver, LoginBrokerSource, CellMessageSender, CellEventListener, CellLifeCycleAware
{
    public static final double EXPIRATION_FACTOR = 2.5;

    /**
     * Map from door names to LoginBrokerInfo of that door. Expired entries are removed
     * lazily when retrieving the list or updating other entries.
     */
    private final ConcurrentMap<String, Entry> doorsByIdentity = new ConcurrentHashMap<>();

    /**
     * Queue of entries in expiration order. There is O(log n) time overhead for every entry due
     * to maintaining this queue. Entries are not removed until expired even if an updated entry
     * has been inserted. Removing old entries upon update would be O(n) time, so we treat a
     * typical 2.5 time increase in memory for lower time complexity.
     */
    private final DelayQueue<Entry> queue = new DelayQueue<>();

    /**
     * Immutable view of unexpired LoginBrokerInfos.
     */
    private final Collection<LoginBrokerInfo> unmodifiableView =
            unmodifiableCollection(transform(filter(doorsByIdentity.values(), Entry::isValid), Entry::getLoginBrokerInfo));

    /**
     * Read doors grouped by protocol.
     */
    private final ByProtocolMap readDoors = new ByProtocolMap();

    /**
     * Write doors grouped by protocol.
     */
    private final ByProtocolMap writeDoors = new ByProtocolMap();

    /**
     * Topic used to request out of order updates.
     */
    private CellAddressCore topic;

    /**
     * Cell endpoint used for communication.
     */
    private CellEndpoint cellEndpoint;

    /**
     * True after receiving a no route to cell error when requesting an update. When true the bean
     * will repeat the request when suitable routes are added.
     */
    private boolean isInitializing;

    /**
     * If non-empty, doors are filtered by these tags.
     */
    private List<String> tags = Collections.emptyList();

    /**
     * Updates are requested from this topic.
     */
    public void setTopic(String topic)
    {
        this.topic = new CellAddressCore(topic);
    }

    /**
     * Doors are filtered by these tags.
     */
    public void setTags(String... tags)
    {
        this.tags = asList(tags);
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        this.cellEndpoint = endpoint;
    }

    @Override
    public void cellCreated(CellEvent ce)
    {
    }

    @Override
    public void cellDied(CellEvent ce)
    {
    }

    @Override
    public void routeAdded(CellEvent ce)
    {
        CellRoute route = (CellRoute) ce.getSource();
        if (route.getRouteType() == CellRoute.TOPIC || route.getRouteType() == CellRoute.DEFAULT) {
            synchronized (this) {
                if (isInitializing) {
                    requestUpdate();
                }
            }
        }
    }

    @Override
    public void routeDeleted(CellEvent ce)
    {
    }

    @Override
    public void afterStart()
    {
        if (topic != null) {
            requestUpdate();
        }
    }

    public void messageArrived(LoginBrokerInfo info)
    {
        expire();
        add(new Entry(info));
    }

    public void messageArrived(NoRouteToCellException e)
    {
        if (e.getDestinationPath().getDestinationAddress().equals(topic)) {
            synchronized (this) {
                isInitializing = true;
            }
        }
    }

    private synchronized void requestUpdate()
    {
        isInitializing = false;
        cellEndpoint.sendMessage(new CellMessage(topic, new LoginBrokerInfoRequest()));
    }

    private void add(Entry entry)
    {
        if (tags.isEmpty() || !Collections.disjoint(tags, entry.getLoginBrokerInfo().getTags())) {
            Entry old = doorsByIdentity.put(entry.info.getIdentifier(), entry);
            queue.add(entry);
            addByProtocol(entry.info);
            if (old != null) {
                removeByProtocol(old.info);
            }
        }
    }

    private void remove(Entry entry)
    {
        LoginBrokerInfo info = entry.getLoginBrokerInfo();
        if (doorsByIdentity.remove(info.getIdentifier(), entry)) {
            removeByProtocol(info);
        }
    }

    private void addByProtocol(LoginBrokerInfo info)
    {
        info.ifCapableOf(READ, readDoors::add);
        info.ifCapableOf(WRITE, writeDoors::add);
    }

    private void removeByProtocol(LoginBrokerInfo info)
    {
        info.ifCapableOf(READ, readDoors::remove);
        info.ifCapableOf(WRITE, writeDoors::remove);
    }

    @Override
    public Collection<LoginBrokerInfo> doors()
    {
        expire();
        return unmodifiableView;
    }

    @Override
    public Map<String, Collection<LoginBrokerInfo>> readDoorsByProtocol()
    {
        expire();
        return readDoors.getUnmodifiable();
    }

    @Override
    public Map<String, Collection<LoginBrokerInfo>> writeDoorsByProtocol()
    {
        expire();
        return writeDoors.getUnmodifiable();
    }

    @Override
    public boolean anyMatch(Predicate<? super LoginBrokerInfo> predicate)
    {
        expire();
        return doorsByIdentity.values().stream().map(Entry::getLoginBrokerInfo).anyMatch(predicate);
    }

    private void expire()
    {
        Entry entry;
        while ((entry = queue.poll()) != null) {
            remove(entry);
        }
    }

    @Command(name = "lb ls", hint = "list collected login broker information")
    class ListCommand implements Callable<String>
    {
        @Option(name = "protocol", usage = "Filter by protocol.")
        String[] protocols;

        @Option(name = "l", usage = "Show time.")
        boolean showTime;

        @Override
        public String call() throws Exception
        {
            Set<String> protocolSet = (protocols != null) ? new HashSet<>(asList(protocols)) : null;
            StringBuilder sb = new StringBuilder();
            for (Entry entry : doorsByIdentity.values()) {
                LoginBrokerInfo info = entry.getLoginBrokerInfo();
                if (protocolSet == null || protocolSet.contains(info.getProtocolFamily())) {
                    sb.append(info);
                    if (showTime) {
                        sb.append(entry.getDelay(MILLISECONDS)).append(" ms;");
                        sb.append(entry.isValid() ? "VALID" : "INVALID").append(';');
                    }
                    sb.append('\n');
                }
            }
            return sb.toString();
        }
    }

    @Command(name = "lb update", hint = "refresh login broker information",
            description = "Semi-blocking command to trigger an update of login brokering " +
                          "information for connected doors. The command blocks until the " +
                          "first reply is received or no doors could be found. Remaining " +
                          "updates are received in the background.")
    class UpdateCommand extends DelayedReply implements Callable<DelayedReply>
    {
        @Override
        public DelayedReply call() throws Exception
        {
            return this;
        }

        @Override
        public void deliver(CellEndpoint endpoint, CellMessage envelope)
        {
            super.deliver(endpoint, envelope);
            cellEndpoint.sendMessage(new CellMessage(topic, new LoginBrokerInfoRequest()),
                                     new CellMessageAnswerable()
                                     {
                                         @Override
                                         public void answerArrived(CellMessage request, CellMessage answer)
                                         {
                                             if (!(answer.getMessageObject() instanceof LoginBrokerInfo)) {
                                                 reply("Invalid reply received: " + answer.getMessageObject());
                                             } else {
                                                 LoginBrokerInfo info = (LoginBrokerInfo) answer.getMessageObject();
                                                 add(new Entry(info));
                                                 reply("Update from " + info.getIdentifier() + " received. Remaining updates are processed in the background.");
                                             }
                                         }

                                         @Override
                                         public void exceptionArrived(CellMessage request,
                                                                      Exception exception)
                                         {
                                             reply("Update failed: " + exception.getMessage());
                                         }

                                         @Override
                                         public void answerTimedOut(CellMessage request)
                                         {
                                             reply("Update timed out.");
                                         }
                                     }, MoreExecutors.directExecutor(), envelope.getAdjustedTtl());
        }
    }

    /**
     * Grouping of doors by protocol. The number of protocols is low enough that we
     * do not bother removing any entries from the map.
     */
    private static class ByProtocolMap
    {
        private final ConcurrentMap<String, Set<LoginBrokerInfo>> doors =
                new ConcurrentHashMap<>();
        private final Map<String, Collection<LoginBrokerInfo>> unmodifiableView =
                unmodifiableMap(Maps.transformValues(
                        Maps.filterValues(doors, (Set<LoginBrokerInfo> set) -> !set.isEmpty()),
                        Collections::unmodifiableCollection));

        public void add(LoginBrokerInfo info)
        {
            get(info.getProtocolFamily()).add(info);
        }

        public boolean remove(LoginBrokerInfo info)
        {
            return get(info.getProtocolFamily()).remove(info);
        }

        public Set<LoginBrokerInfo> get(String protocol)
        {
            return doors.computeIfAbsent(protocol, key -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
        }

        public Map<String, Collection<LoginBrokerInfo>> getUnmodifiable()
        {
            return unmodifiableView;
        }
    }

    private static class Entry implements Delayed
    {
        private final long expirationTime;
        private final LoginBrokerInfo info;

        public Entry(LoginBrokerInfo info)
        {
            this.expirationTime = System.currentTimeMillis() + (long) (EXPIRATION_FACTOR * info.getUpdateTime());
            this.info = info;
        }

        public LoginBrokerInfo getLoginBrokerInfo()
        {
            return info;
        }

        public boolean isValid()
        {
            return expirationTime > System.currentTimeMillis();
        }

        @Override
        public long getDelay(TimeUnit unit)
        {
            return unit.convert(expirationTime - System.currentTimeMillis(), MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o)
        {
            return Long.compare(expirationTime, ((Entry) o).expirationTime);
        }
    }
}
