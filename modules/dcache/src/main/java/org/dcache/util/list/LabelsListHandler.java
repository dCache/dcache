/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
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
package org.dcache.util.list;

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import dmg.cells.nucleus.CellMessageReceiver;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.CacheExceptionFactory;

import org.dcache.vehicles.PnfsListLabelsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LabelsListHandler which delegates the label listing operation to the PnfsManager.
 * <p>
 * Large number of label listing is broken into several reply messages by the PnfsManager. For that reason the
 * regular Cells callback mechanism for replies cannot be used. Instead messages of type
 * PnfsListLabelsMessage must be routed to the LabelsListHandler. This also has the
 * consequence that a LabelsListHandler cannot be used from the Cells messages thread. Any
 * attempt to do so will cause the message thread to block, as the replies cannot be delivered to
 * the LabelsListHandler.
 */
public class LabelsListHandler
        implements CellMessageReceiver, LabelsListSource {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ListDirectoryHandler.class);

    private final PnfsHandler _pnfs;
    private final Map<UUID, Stream> _replies =
            new ConcurrentHashMap<>();

    public LabelsListHandler(PnfsHandler pnfs) {
        _pnfs = pnfs;
    }


    @Override
    public DirectoryStream
    listLabels(Subject subject, Restriction restriction,
                         Range<Integer> range, Set<FileAttribute> attributes)
            throws InterruptedException, CacheException
    {
        PnfsListLabelsMessage msg =
                new PnfsListLabelsMessage( range, attributes);
        UUID uuid = msg.getUUID();
        boolean success = false;
        Stream stream = new Stream(uuid);
        try {
            msg.setSubject(subject);
            msg.setRestriction(restriction);
            _replies.put(uuid, stream);
            _pnfs.send(msg);
            stream.waitForMoreEntries();
            success = true;
            return stream;
        } finally {
            if (!success) {
                _replies.remove(uuid);
            }
        }
    }

    /**
     * Callback for delivery of replies from PnfsManager. PnfsListLabelsMessage have to be routed
     * to this message.
     */
    public void messageArrived(PnfsListLabelsMessage reply) {
        if (reply.isReply()) {
            try {
                UUID uuid = reply.getUUID();
                LabelsListHandler.Stream stream = _replies.get(uuid);
                if (stream != null) {
                    stream.put(reply);
                } else {
                    LOGGER.warn(
                            "Received list result for an unknown request. Labels listing was possibly incomplete.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    /**
     * Implementation of DirectoryStream, translating PnfsListLabelsMessage replies to a stream
     * of DirectoryEntries.
     * <p>
     * The stream acts as its own iterator, and multiple iterators are not supported.
     */
    public class Stream
            implements DirectoryStream, Iterator<DirectoryEntry> {

        private final BlockingQueue<PnfsListLabelsMessage> _queue =
                new LinkedBlockingQueue<>();
        private final UUID _uuid;
        private boolean _isFinal;
        private Iterator<DirectoryEntry> _iterator;
        private int _count;
        private int _total;

        public Stream(UUID uuid) {
            _uuid = uuid;
        }

        @Override
        public void close() {
            _replies.remove(_uuid);
        }

        private void put(PnfsListLabelsMessage msg)
                throws InterruptedException {
            _queue.put(msg);
        }

        private void waitForMoreEntries()
                throws InterruptedException, CacheException {
            if (_isFinal) {
                _iterator = null;
                return;
            }

            PnfsListLabelsMessage msg =
                    _queue.poll(_pnfs.getPnfsTimeout(), TimeUnit.MILLISECONDS);
            if (msg == null) {
                throw new CacheException(CacheException.TIMEOUT,
                        "Timeout during labels listing.");
            }

            if (msg.isFinal()) {
                _total = msg.getMessageCount();
            }
            _count++;
            if (_count == _total) {
                _isFinal = true;
            }

            if (msg.getReturnCode() != 0) {
                throw CacheExceptionFactory.exceptionOf(msg);
            }

            _iterator = msg.getEntries().iterator();

            /* If the message is empty, then the iterator has no next
             * element. In that case we wait for the next reply. This
             * may in particular happen with the final message.
             */
            if (!_iterator.hasNext()) {
                waitForMoreEntries();
            }
        }

        @Override
        public Iterator<DirectoryEntry> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            try {
                if (_iterator == null || !_iterator.hasNext()) {
                    waitForMoreEntries();
                    if (_iterator == null) {
                        return false;
                    }
                }
            } catch (CacheException e) {
                LOGGER.error("Listing of labels incomplete: {}", e.getMessage());
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            return true;
        }

        @Override
        public DirectoryEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return _iterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
