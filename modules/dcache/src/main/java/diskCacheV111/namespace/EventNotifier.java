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
package diskCacheV111.namespace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.io.BaseEncoding;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.dcache.namespace.events.EventType;

import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;
import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.events.Event;
import org.dcache.events.NotificationMessage;
import org.dcache.events.SystemEvent;
import org.dcache.namespace.FileType;
import org.dcache.namespace.events.InotifyEvent;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Multimaps.synchronizedListMultimap;

/**
 * This class is responsible for accepting inotify(7)-like events and sending
 * them to event receivers: cells within dCache.  An event receiver must
 * subscribe (so selecting the subset of events that are of interest) before it
 * receives any events.
 * <p>
 * The two main design goals are that the thread delivering an event to this
 * class never blocks (see EventReceiver) and that the system "degrades
 * gracefully" under overload.  The work is handled by executors, allowing the
 * CPU resources are limited.  This ensures that sending events can never
 * monopolise activity, preventing dCache from doing useful work.  Instead,
 * under overload, users will experience a delay in receiving events (as events
 * are queued).  If the overload persists then events will be dropped, which
 * users are informed of through the OVERFLOW event.
 * <p>
 * Note that, it is possible (under heavy load) that some undesired events are
 * sent to an event receive if the event receiver's subscription changes and
 * the events have already been queued for delivery.
 * <p>
 * Subscription is handled through ZooKeeper: the event receiver updates a
 * ZK node using an encoded version of the desired events.  ZK places a limit
 * on the size of any node, which limits the number of concurrent "watches" any
 * event receiver may make.  The binary format of the ZK node is versioned,
 * to support future changes.
 * <p>
 * Incoming events are immediately queued for processing.  This avoids blocking
 * while this class is processing any changes in the watches/subscriptions.  If
 * this queue exceeds the maximum allowed size then all event receivers will
 * receive the OVERFLOW event.
 * <p>
 * Queued events are processed to determine which (if any) event receivers are
 * interested.  Each event receiver has a queue of events: those that match this
 * event receiver's list of watches.  These events are sent to the event
 * receiver as a sequence of messages directed explicitly to that event receiver.
 * <p>
 * A single threaded task is responsible for sending events.  This task sends
 * all outstanding events for an event receiver before moving onto the next
 * event receiver.  To reduce overheads in message sending, multiple events may
 * be sent in a single message.  The number of events sent in any one message
 * is limited to prevent sending very large messages.
 */
public class EventNotifier implements EventReceiver, CellMessageReceiver,
        CuratorFrameworkAware, CellLifeCycleAware, PathChildrenCacheListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EventNotifier.class);
    public static final String INOTIFY_PATH = ZKPaths.makePath("dcache", "inotify");
    private static final Event OVERFLOW_EVENT = new SystemEvent(SystemEvent.Type.OVERFLOW);

    /**
     * Convert an event receiver's list of desired events to the binary
     * representation.  Each Map.Entry is a Watch subscribing to some events of
     * some file or directory.
     * @param entries the watches.  The watches to which the event receiver is
     * subscribing.
     * @return the binary data for the ZooKeeper node.
     */
    public static byte[] toZkData(Map<PnfsId,Collection<EventType>> entries)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0); // Format #0: simple list.
        for (Map.Entry<PnfsId,Collection<EventType>> entry : entries.entrySet()) {
            // 16-bit mask of interested events; currently 13 of 16 bits are used.
            short bitmask = (short) entry.getValue().stream()
                    .mapToInt(EventType::ordinal)
                    .map(o -> 1<<o)
                    .reduce(0, (a,b) -> a | b);
            out.write(bitmask >> 8);
            out.write(bitmask & 0xff);

            PnfsId id = entry.getKey();
            // REVISIT this assumes that the PNFS-ID value is (upper case) hex value.
            byte[] pnfsid = BaseEncoding.base16().decode(id.toString());
            checkState(pnfsid.length < 256, "PNFS-ID length exceeds 256 bytes");
            out.write((byte)pnfsid.length);
            out.write(pnfsid, 0, pnfsid.length);
            LOGGER.debug("encoded id={} bitmask={}", id, bitmask);
        }
        return out.toByteArray();
    }

    /**
     * Convert a binary representation back into an event receiver's list of
     * desired events.
     * @param data the binary data
     * @return a Map between the target and its set of desired event types.
     * @throws IllegalArgumentException if the data is badly formatted.
     */
    public static Map<PnfsId,EnumSet<EventType>> fromZkData(byte[] data)
    {
        checkArgument(data.length > 1, "Too little data");
        checkArgument(data[0] == 0, "Wrong format");

        Map<PnfsId,EnumSet<EventType>> deserialised = new HashMap<>();
        int index = 1;
        while (index < data.length) {
            checkArgument(data.length - index >= 3, "Too little data for bitmask");
            short bitmask = (short)(data [index++] << 8 | data [index++] & 0xFF);

            EnumSet<EventType> eventTypes = Arrays.stream(EventType.values())
                    .filter(t -> (bitmask & 1<<t.ordinal()) != 0)
                    .collect(Collectors.toCollection(() -> EnumSet.noneOf(EventType.class)));

            byte length = data [index++];
            checkArgument(data.length - index >= length, "Too little data for PNFSID");
            PnfsId id = new PnfsId(BaseEncoding.base16().encode(data, index, length));
            index += length;
            deserialised.put(id, eventTypes);
            LOGGER.debug("Adding id={} bitmask={} types={}", id, bitmask, eventTypes);
        }

        return deserialised;
    }

    /**
     * The current state of the sender task.
     */
    private enum SenderState {
        /** Currently no sender task. */
        NOT_RUNNING,
        /** Sender task is running and will terminate once finished. */
        FINAL_RUN,
        /** Sender task is running and will start another run once the current run completes. */
        NONFINAL_RUN
    }

    /**
     * Represents an event receiver's subscription to events that target some
     * specific file or directory.  The name Watch is used as this class is
     * somewhat similar to the inotify(7) concept.
     */
    private class Watch
    {
        private final PnfsId target;
        private final EnumSet<EventType> selectedEvents;
        private final Consumer<Event> eventSender;

        public Watch(PnfsId target, EnumSet<EventType> events,
                Consumer<Event> sender)
        {
            this.target = target;
            eventSender = sender;
            selectedEvents = EnumSet.copyOf(events);
        }

        public void accept(EventType eventType, String name, String cookie, FileType type)
        {
            if (selectedEvents.contains(eventType)) {
                eventSender.accept(new InotifyEvent(eventType, target, name, cookie, type));
            }
        }
    }

    /**
     * Represents some cell within dCache that has requested inotify events.
     * Each event receiver has an independent queue.  This allows a "busy"
     * event receiver (one that has watches that are receiving many events) to
     * overflow without affecting "quiet" event receivers.
     */
    private class EventReceiver
    {
        private final Queue<Event> in = new ArrayBlockingQueue(maximumQueuedEvents);
        private final List<Watch> watches = new ArrayList<>();
        private boolean overflow;

        public EventReceiver(byte[] data)
        {
            fromZkData(data).forEach((id,flags) ->
                    watches.add(new Watch(id, flags, this::enqueueMessage)));
        }

        private void sendOverflow()
        {
            // A subtle point.  The enqueueMessasge method will not enqueue this
            // OVERFLOW event if enqueueing the message results in the queue
            // overflowing, or if the queue is already in overflow.  In either
            // case, the event receiver will receive (or will have received) an
            // OVERFLOW event, so this failure does not matter.
            enqueueMessage(OVERFLOW_EVENT);
        }

        public synchronized void enqueueMessage(Event event)
        {
            if (in.offer(event)) {
                notifyEventToSend();
            } else if (!overflow) {
                LOGGER.warn("Inotify overflow: too slow sending events");
                overflow = true;
            }
        }

        public synchronized List<Event> drainQueuedEvents()
        {
            int count = in.size();
            if (overflow) {
                count++;
            }

            if (count == 0) {
                return Collections.emptyList();
            }

            List<Event> todo = new ArrayList<>(count);

            todo.addAll(in);
            if (overflow) {
                todo.add(OVERFLOW_EVENT);
            }
            in.clear();
            overflow = false;

            return todo;
        }
    }

    private CellStub eventSender;
    private PathChildrenCache cache;
    private final Map<CellAddressCore,EventReceiver> receivers = new ConcurrentHashMap<>();
    private final ListMultimap<PnfsId, Watch> watchesByPnfsId =
            synchronizedListMultimap(MultimapBuilder.hashKeys().arrayListValues().build());
    private final AtomicReference<SenderState> senderTask =
            new AtomicReference<>(SenderState.NOT_RUNNING);

    private Executor dispatchExecutor;
    private Executor senderExecutor;
    private boolean dispatchOverflow;
    private boolean overflowNotificationScheduled;
    private int batchSize;
    private int maximumQueuedEvents;

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        setPathChildrenCache(new PathChildrenCache(client, INOTIFY_PATH, true));
    }

    @VisibleForTesting
    void setPathChildrenCache(PathChildrenCache cache)
    {
        this.cache = cache;
    }

    @Required
    public void setDispatchExecutor(Executor executor)
    {
        dispatchExecutor = executor;
    }

    @Required
    public void setSenderExecutor(Executor executor)
    {
        senderExecutor = executor;
    }

    @Required
    public void setMaximumQueuedEvents(int maximum)
    {
        maximumQueuedEvents = maximum;
    }

    @Required
    public void setEventBatchSize(int size)
    {
        checkArgument(size > 0, "Cannot be zero or negative value");
        batchSize = size;
    }

    @Override
    public void afterStart()
    {
        LOGGER.debug("EventNotifier#afterStart");
        cache.getListenable().addListener(this);
        try {
            cache.start();
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beforeStop()
    {
        LOGGER.debug("EventNotifier#beforeStop");
        CloseableUtils.closeQuietly(cache);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
        ChildData child = event.getData();
        String name = ZKPaths.getNodeFromPath(child.getPath());
        CellAddressCore target = new CellAddressCore(name);

        switch (event.getType()) {
        case CHILD_ADDED:
        case CHILD_UPDATED:
            LOGGER.debug("ZK child {} added or updated", name);
            EventReceiver priorReceiver = receivers.get(target);
            EventReceiver receiver = new EventReceiver(child.getData());

            synchronized (watchesByPnfsId) {
                if (priorReceiver != null) {
                    priorReceiver.watches.forEach(w -> watchesByPnfsId.remove(w.target, w));
                }

                receivers.put(target, receiver);
                receiver.watches.forEach(w -> watchesByPnfsId.put(w.target, w));
            }
            break;

        case CHILD_REMOVED:
            LOGGER.debug("ZK child {} removed", name);
            EventReceiver oldReceiver = receivers.remove(target);
            synchronized (watchesByPnfsId) {
                if (oldReceiver != null) {
                    oldReceiver.watches.forEach(w -> watchesByPnfsId.remove(w.target, w));
                }
            }
            break;

        default:
            LOGGER.debug("unexpected ZK event: {}", event.getType());
        }
    }

    @Required
    public void setCellStub(CellStub subscribers)
    {
        this.eventSender = subscribers;
    }

    private void acceptEvent(EventType eventType, PnfsId target, String name,
            String cookie, FileType fileType)
    {
        synchronized (this) {
            if (dispatchOverflow && !overflowNotificationScheduled) {
                try {
                    dispatchExecutor.execute(this::notifyDispatchOverflow);
                    overflowNotificationScheduled = true;
                } catch (RejectedExecutionException e) {
                    LOGGER.debug("Rejected dispatch overflow notification");
                    // OK, just try again with the next acceptEvent call.
                }
            }
        }
        List<Watch> watches = watchesByPnfsId.get(target);
        synchronized (watchesByPnfsId) {
            watches.forEach(w -> w.accept(eventType, name, cookie, fileType));
        }
    }

    private synchronized void notifyDispatchOverflow()
    {
        receivers.values().forEach(EventReceiver::sendOverflow);
        dispatchOverflow = false;
    }

    @Override
    public void notifySelfEvent(EventType eventType, PnfsId target, FileType fileType)
    {
        notifyEvent(eventType, target, null, null, fileType);
    }

    @Override
    public void notifyChildEvent(EventType eventType, PnfsId target, String name, FileType fileType)
    {
        notifyEvent(eventType, target, name, null, fileType);
    }

    @Override
    public void notifyMovedEvent(EventType eventType, PnfsId target, String name, String cookie, FileType fileType)
    {
        notifyEvent(eventType, target, name, cookie, fileType);
    }

    private synchronized void notifyEvent(EventType eventType, PnfsId target, String name, String cookie, FileType fileType)
    {
        if (!dispatchOverflow) {
            try {
                // The acceptEvent is decoupled via the dispatchExecutor to
                // prevent this method from blocking if acceptEvent blocks.  The
                // acceptEvent method can block if the set of watches (as
                // recorded in ZK) changes.
                dispatchExecutor.execute(() -> acceptEvent(eventType, target, name, cookie, fileType));
            } catch (RejectedExecutionException e) {
                LOGGER.warn("Inotify overflow: too slow accepting new events");
                dispatchOverflow = true;
                overflowNotificationScheduled = false;
            }
        }
    }

    private void notifyEventToSend()
    {
        if (senderTask.compareAndSet(SenderState.FINAL_RUN, SenderState.NONFINAL_RUN)) {
            LOGGER.debug("Sender task updated to non-final run");
        } else if (senderTask.compareAndSet(SenderState.NOT_RUNNING, SenderState.FINAL_RUN)) {
            LOGGER.debug("Executing sender task");
            senderExecutor.execute(this::sendQueuedEvents);
        }
    }

    private void sendQueuedEvents()
    {
        do {
            LOGGER.debug("Sender task starting send run");
            senderTask.set(SenderState.FINAL_RUN);

            receivers.forEach((door, receiver) -> {
                        List<Event> events = receiver.drainQueuedEvents();

                        CellPath path = new CellPath(door);

                        int sent = 0;
                        while (sent < events.size()) {
                            int upper = Math.min(events.size(), sent+batchSize);
                            List<Event> batch = events.subList(sent, upper);
                            LOGGER.debug("Sending events: {}", batch);
                            eventSender.notify(path, new NotificationMessage(batch));
                            sent += batch.size();
                        }
                    });

        } while (!senderTask.compareAndSet(SenderState.FINAL_RUN, SenderState.NOT_RUNNING));
        LOGGER.debug("Sender task terminating");
    }
}
