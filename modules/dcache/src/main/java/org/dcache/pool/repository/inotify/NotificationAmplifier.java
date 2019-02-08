/* dCache - http://www.dcache.org/
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
package org.dcache.pool.repository.inotify;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.curator.shaded.com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.UriUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import diskCacheV111.namespace.EventReceiver;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolIoFileMessage;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.namespace.events.EventType;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.vehicles.FileAttributes;

/**
 * This class is responsible for accepting inotify events that target some
 * specific file and emits events that target that file and the corresponding
 * event for the parent directory.
 */
public class NotificationAmplifier extends AbstractStateChangeListener implements CellMessageReceiver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationAmplifier.class);

    /** We expect most files to appear only once in the namespace. */
    private static final int EXPECTED_LINKS_PER_FILE = 1;

    /**
     * Represents the location of a file within the namespace.
     */
    private static class Link {
        private final PnfsId parent;
        private final String name;

        public Link(PnfsId parent, String name)
        {
            this.parent = parent;
            this.name = name;
        }
    }

    private final Multimap<PnfsId,Link> namespace =
            MultimapBuilder.hashKeys().arrayListValues(EXPECTED_LINKS_PER_FILE).build();
    private final EventReceiver receiver;

    public NotificationAmplifier(EventReceiver receiver)
    {
        this.receiver = receiver;
    }

    public void messageArrived(PoolIoFileMessage message)
    {
        LOGGER.debug("messageArrived: {}", message);
        FileAttributes attributes = message.getFileAttributes();
        extractSerialisedLinks(attributes)
                .map(NotificationAmplifier::decodeLinks)
                .ifPresent(l -> updateLinks(attributes.getPnfsId(), l));
    }

    private void updateLinks(PnfsId id, List<Link> newLinks)
    {
        LOGGER.debug("Updating stored namespace links for {}", id);
        synchronized (namespace) {
            Collection<Link> existingLinks = namespace.get(id);
            existingLinks.clear();
            existingLinks.addAll(newLinks);
        }
    }

    private static Optional<String> extractSerialisedLinks(FileAttributes attributes)
    {
        return attributes.isDefined(FileAttribute.STORAGEINFO)
                ? Optional.ofNullable(attributes.getStorageInfo().getKey("links"))
                : Optional.empty();
    }

    private static List<Link> decodeLinks(String serialisedLinks)
    {
        List<Link> links = new ArrayList<>();
        for (String serialisedLink : Splitter.on('#').split(serialisedLinks)) {
            List<String> items = Splitter.on(' ').limit(2).splitToList(serialisedLink);
            PnfsId parent = new PnfsId(items.get(0));
            String name = UriUtils.decode(items.get(1), StandardCharsets.UTF_8);
            links.add(new Link(parent, name));
        }
        return links;
    }

    public void sendEvent(PnfsId id, EventType event)
    {
        LOGGER.debug("Sending {} event for {} and parents", event, id);
        synchronized (namespace) {
            for (Link link : namespace.get(id)) {
                receiver.notifyChildEvent(event, link.parent, link.name, FileType.REGULAR);
            }
        }
        receiver.notifySelfEvent(event, id, FileType.REGULAR);
    }

    @Override
    public void stateChanged(StateChangeEvent event)
    {
        if (event.getNewState() == ReplicaState.DESTROYED) {
            LOGGER.debug("cleaning up {}", event.getPnfsId());
            synchronized (namespace) {
                namespace.removeAll(event.getPnfsId());
            }
        }
    }
}