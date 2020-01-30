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
package org.dcache.restful.events.streams.inotify;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import diskCacheV111.namespace.EventNotifier;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.events.Event;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.events.InotifyEvent;
import org.dcache.namespace.events.EventType;
import org.dcache.events.NotificationMessage;
import org.dcache.events.SystemEvent;
import org.dcache.restful.events.spi.EventStream;
import org.dcache.restful.events.spi.SelectedEventStream;
import org.dcache.restful.events.spi.SelectionResult;
import org.dcache.restful.util.RequestUser;
import org.dcache.vehicles.FileAttributes;

import static diskCacheV111.namespace.EventNotifier.INOTIFY_PATH;
import static org.dcache.auth.attributes.Activity.READ_METADATA;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.restful.util.transfers.Json.readFromJar;

/**
 * An EventStream that provides the client with events based on namespace
 * activity.  The dCache-internal events that this EventStream provides are
 * closely modelled on the inotify(7) API, from Linux.
 * <p>
 * Care must be taken when entering the two monitors: Inotify and
 * InotifySelection.  To avoid lock inversions (ABBA-type deadlocks), we must
 * guarantee that the Inotify monitor is <emph>NEVER</emph> entered after a
 * thread has entered the InotifySelection monitor.
 * <p>
 * One way to achieve this is to always enter the Inotify monitor before
 * entering the InotifySelection monitor.  This is sufficient to guarantee
 * the code is free of such ABBA deadlocks, but this isn't strictly necessary.
 * If the code can guarantee that the Inotify monitor is never entered then it
 * is safe to enter the InotifySelection monitor without first entering the
 * Inotify monitor.
 */
public class Inotify implements EventStream, CellMessageReceiver,
        CuratorFrameworkAware, CellIdentityAware, CellLifeCycleAware
{
    /**
     * Represents the kind of inotify events selected by a specific client from
     * a specific PNFS-ID.  This is the client-supplied selector plus some
     * operational field-members.
     */
    private class InotifySelection implements SelectedEventStream
    {
        private final BiConsumer<String,JsonNode> eventReceiver;
        private final WatchIdentity identity;
        private final Restriction restriction;
        private Set<EventType> selectedEvents;
        private InotifySelector selector;
        private boolean isClosed;

        private InotifySelection(BiConsumer<String,JsonNode> receiver,
                InotifySelector selector, WatchIdentity id) throws CacheException,
                InterruptedException, NoRouteToCellException
        {
            this.eventReceiver = receiver;
            this.selector = selector;
            identity = id;
            restriction = RequestUser.getRestriction();
            selectedEvents = AddWatchFlag.asEventType(selector.flags());
        }

        public void accept(InotifySelector selector) throws CacheException,
                InterruptedException, NoRouteToCellException
        {
            if (selector.flags().contains(AddWatchFlag.IN_MASK_ADD)) {
                InotifySelector combinedSelector = new InotifySelector();
                combinedSelector.setPath(selector.getPath());

                EnumSet<AddWatchFlag> newFlags = EnumSet.copyOf(selector.flags());
                newFlags.addAll(this.selector.flags());
                newFlags.remove(AddWatchFlag.IN_MASK_ADD);
                combinedSelector.setSuppliedFlags(newFlags);

                this.selector = combinedSelector;
            } else {
                this.selector = selector;
            }
            selectedEvents = AddWatchFlag.asEventType(selector.flags());
        }

        @Override
        public String getId()
        {
            return identity.selectionId();
        }

        private Set<EventType> selectedEvents()
        {
            return selectedEvents;
        }

        @Override
        public JsonNode selector()
        {
            return mapper.convertValue(selector, JsonNode.class);
        }

        public synchronized void requestClose()
        {
            if (!isClosed) {
                sendEvent(EventStream.CLOSE_STREAM);
            }
        }

        @Override
        public void close()
        {
            boolean justClosed;

            synchronized (this) {
                /* IMPORTANT: must NOT enter the Inotify monitor while inside
                 * this InotifySelection monitor. */
                justClosed = !isClosed;

                if (justClosed) {
                    sendEvent(IN_IGNORED_JSON);
                    isClosed = true;
                }
            }

            if (justClosed) {
                synchronized (Inotify.this) {
                    selectionByWatch.remove(identity);
                    selectionByPnfsId.remove(identity.pnfsid(), this);
                    try {
                        updateZK();
                    } catch (CacheException e) {
                        LOGGER.error("Unsubscription failed: {}", e.toString());
                    }
                }
            }
        }

        @GuardedBy("this")
        private void sendEvent(JsonNode json)
        {
            /* MUST NOT cause this thread to enter the Inotify monitor. */
            eventReceiver.accept(identity.selectionId(), json);
        }

        public synchronized boolean isSendableEvent(EventType type, String filename)
        {
            FsPath target = selector.getFsPath();

            boolean isRestricted = filename == null
                    ? restriction.isRestricted(READ_METADATA, target.parent(), target.name())
                    : restriction.isRestricted(READ_METADATA, target, filename);

            return !isClosed && selectedEvents.contains(type) && !isRestricted;
        }

        public synchronized boolean isClosed()
        {
            return isClosed;
        }

        private synchronized void acceptEvent(JsonNode json)
        {
            sendEvent(json);

            if (selector.flags().contains(AddWatchFlag.IN_ONESHOT)) {
                requestClose();
            }
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Inotify.class);
    private static final ObjectNode SELECTOR_SCHEMA = readFromJar("/org/dcache/frontend/inotify/selector-schema.json");
    private static final ObjectNode EVENT_SCHEMA = readFromJar("/org/dcache/frontend/inotify/event-schema.json");

    public static final JsonNode IN_IGNORED_JSON =
            new ObjectMapper().convertValue(new InotifyEventPayload(EventFlag.IN_IGNORED), JsonNode.class);

    private final Map<WatchIdentity,InotifySelection> selectionByWatch = new ConcurrentHashMap<>();
    private final SetMultimap<PnfsId,InotifySelection> selectionByPnfsId =
            MultimapBuilder.hashKeys().hashSetValues().build();
    private final ObjectMapper mapper = new ObjectMapper();

    private PnfsHandler pnfsHandler;
    private CuratorFramework curator;
    private String subscriptionPath;
    private PersistentNode subscriptions;

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        curator = client;
    }

    @Override
    public String eventType()
    {
        return "inotify";
    }

    @Override
    public String description()
    {
        return "notification of namespace activity, modelled after inotify(7)";
    }

    @Required
    public void setPnfsHandler(PnfsHandler handler)
    {
        pnfsHandler = handler;
    }

    @Override
    public ObjectNode selectorSchema()
    {
        return SELECTOR_SCHEMA;
    }

    @Override
    public ObjectNode eventSchema()
    {
        return EVENT_SCHEMA;
    }

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        subscriptionPath = ZKPaths.makePath(INOTIFY_PATH, address.toString());
    }

    @Override
    public void afterStart()
    {
        try {
            curator.create().withMode(CreateMode.PERSISTENT).forPath(INOTIFY_PATH);
        } catch (NodeExistsException e) {
            // It is OK for the node to exist already.
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("Unable to create 'inotify' node: " + e);
        }
    }


    private void updateZK() throws CacheException
    {
        try {
            if (selectionByPnfsId.isEmpty()) {
                if (subscriptions != null) {
                    LOGGER.debug("Deleting ZK node.");
                    subscriptions.close();
                    subscriptions = null;
                }
            } else {
                byte[] data = encodeSubscriptions();
                if (subscriptions == null) {
                    LOGGER.debug("Creating new ZK node with {} bytes.",
                            data.length);
                    subscriptions = new PersistentNode(curator,
                            CreateMode.EPHEMERAL, false, subscriptionPath, data);
                    subscriptions.start();
                    subscriptions.waitForInitialCreate(10, TimeUnit.SECONDS);
                } else {
                    LOGGER.debug("Updating data in ZK node to contain {} bytes.",
                            data.length);
                    subscriptions.setData(data);
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (InterruptedException e) {
            throw new CacheException("Failed to create ZooKeeper node: " + e);
        } catch (Exception e) {
            throw new CacheException("Failed to update ZooKeeper: " + e);
        }
    }

    private byte[] encodeSubscriptions()
    {
        Map<PnfsId,Collection<EventType>> selectedEvents = new HashMap<>();

        selectionByPnfsId.asMap().forEach((id,selections) -> {
                    // Always subscribe to IN_DELETE_SELF to discover when to close subscription
                    Collection<EventType> events = EnumSet.of(EventType.IN_DELETE_SELF);
                    selections.forEach(s -> events.addAll(s.selectedEvents()));
                    selectedEvents.put(id, events);
                });

        return EventNotifier.toZkData(selectedEvents);
    }

    public void messageArrived(NotificationMessage message)
    {
        message.forEachEvent(this::acceptEvent);
    }

    private synchronized void acceptEvent(Event rawEvent)
    {
        LOGGER.debug("accepting event: {}", rawEvent);

        switch (rawEvent.getCategory()) {

        case "inotify":
            InotifyEvent inotifyEvent = (InotifyEvent) rawEvent;
            EventType type = inotifyEvent.getEventType();
            String filename = inotifyEvent.getName();

            Collection<InotifySelection> selections = inotifyEvent.getTarget() == null
                    ? selectionByWatch.values()
                    : selectionByPnfsId.get(inotifyEvent.getTarget());

            if (selections.stream().anyMatch(s -> s.isSendableEvent(type, filename))) {
                JsonNode json = mapper.convertValue(new InotifyEventPayload(inotifyEvent),
                        JsonNode.class);
                selections.stream()
                        .filter(s -> s.isSendableEvent(type, filename))
                        .forEach(s -> s.acceptEvent(json));
            }

            if (type.equals(EventType.IN_DELETE_SELF)) {
                selections.forEach(InotifySelection::requestClose);
            }
            break;

        case "SYSTEM":
            SystemEvent systemEvent = (SystemEvent) rawEvent;
            JsonNode json = mapper.convertValue(new InotifyEventPayload(systemEvent), JsonNode.class);
            selectionByWatch.values().stream()
                    .filter(s -> !s.isClosed())
                    .forEach(s -> s.acceptEvent(json));
            break;
        }
    }

    @Override
    public SelectionResult select(String channelId, BiConsumer<String,JsonNode> receiver,
            JsonNode serialisedSelector)
    {
        try {
            InotifySelector selector = mapper.readerFor(InotifySelector.class)
                    .readValue(serialisedSelector);
            return selector.validationError().orElseGet(() -> select(channelId, receiver, selector));
        } catch (JsonMappingException e) {
            int index = e.getMessage().indexOf('\n');
            String msg = index == -1 ? e.getMessage() : e.getMessage().substring(0, index);
            return SelectionResult.badSelector("Bad selector value: %s", msg);
        } catch (IOException e) {
            return SelectionResult.badSelector("Unable to process selector: " + e.getMessage());
        }
    }

    private synchronized SelectionResult select(String channelId, BiConsumer<String,JsonNode> receiver,
            InotifySelector selector)
    {
        try {
            PnfsId pnfsid = lookup(selector);
            WatchIdentity watchId = new WatchIdentity(channelId, pnfsid);
            InotifySelection selection = selectionByWatch.get(watchId);
            if (selection == null) {
                selection = new InotifySelection(receiver, selector, watchId);
                selectionByWatch.put(watchId, selection);
                selectionByPnfsId.put(pnfsid, selection);
                updateZK();
                return SelectionResult.created(selection);
            } else {
                selection.accept(selector);
                updateZK();
                return SelectionResult.merged(selection);
            }
        } catch (PermissionDeniedCacheException e) {
            return SelectionResult.permissionDenied("Not allowed.");
        } catch (FileNotFoundCacheException e) {
            return SelectionResult.resourceNotFound("Problem with path: " + e.getMessage());
        } catch (NotDirCacheException e) {
            return SelectionResult.conditionFailed(e.getMessage());
        } catch (CacheException | NoRouteToCellException e) {
            return SelectionResult.internalError(e.getMessage());
        } catch (InterruptedException e) {
            return SelectionResult.internalError("Service going down");
        }
    }

    private PnfsId lookup(InotifySelector selector) throws CacheException
    {
        FsPath path = selector.getFsPath();
        PnfsHandler handler = new PnfsHandler(pnfsHandler,
                RequestUser.getSubject(), RequestUser.getRestriction());

        FileAttributes attr = handler.getFileAttributes(path, EnumSet.of(PNFSID, TYPE));

        // REVISIT can we avoid the second round-trip by enhancing the AccessMask
        // concept?  This would need semantics like:
        //
        //     if FileType == DIR then LIST_DIRECTORY otherwise READ_DATA.

        if (attr.getFileType() == FileType.DIR) {
            handler.getFileAttributes(path.toString(), // REVISIT update PnfsHandler to support FsPath here
                    EnumSet.noneOf(FileAttribute.class),
                    EnumSet.of(AccessMask.LIST_DIRECTORY),
                    false);
        } else {
            if (selector.flags().contains(AddWatchFlag.IN_ONLYDIR)) {
                throw new NotDirCacheException("IN_ONLYDIR with " + attr.getFileType() + " target");
            }

            handler.getFileAttributes(path.toString(),
                    EnumSet.noneOf(FileAttribute.class),
                    EnumSet.of(AccessMask.READ_DATA),
                    false);
        }

        return attr.getPnfsId();
    }
}
