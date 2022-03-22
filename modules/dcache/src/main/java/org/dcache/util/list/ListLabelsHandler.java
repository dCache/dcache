package org.dcache.util.list;

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import dmg.cells.nucleus.CellMessageReceiver;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.Glob;
import org.dcache.vehicles.PnfsListLabelsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DirectoryListSource which delegates the list operation to the PnfsManager.
 * <p>
 * When labels list is large it is  broken into several reply messages by the PnfsManager. For that
 * reason the regular Cells callback mechanism for replies cannot be used. Instead messages of type
 * PnfsLabelsMessage must be routed to the ListlabelsyHandler. This also has the consequence that a
 * ListDirectoryHandler cannot be used from the Cells messages thread. Any attempt to do so will
 * cause the message thread to block, as the replies cannot be delivered to the ListLabelsHandler.
 */
public class ListLabelsHandler
      implements CellMessageReceiver, LabelsListSource {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(ListDirectoryHandler.class);

    private final PnfsHandler _pnfs;
    private final Map<UUID, StreamLabels> _replies =
          new ConcurrentHashMap<>();

    public ListLabelsHandler(PnfsHandler pnfs) {
        _pnfs = pnfs;
    }


    /**
     * Sends a lable's list request to PnfsManager. The result is provided as a stream of
     * files having the label value equale to the value of path param.
     * <p>
     * <p>
     * Note that supplied subject and restriction values will be overwritten if {@link
     * PnfsHandler#setSubject} or {@link PnfsHandler#setRestriction} have been called on the
     * underlying PnfsHandler instance.
     */

    @Override
    public LabelsStream
    listLabels(Subject subject, Restriction restriction, Glob pattern,
          Range<Integer> range)
          throws InterruptedException, CacheException {
        PnfsListLabelsMessage msg =
              new PnfsListLabelsMessage(null, range);
        UUID uuid = msg.getUUID();
        boolean success = false;
        StreamLabels stream = new StreamLabels(uuid);
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
     * Callback for delivery of replies from PnfsManager. PnfsLabelsMessage have to be routed
     * to this message.
     */
    public void messageArrived(PnfsListLabelsMessage reply) {
        if (reply.isReply()) {
            try {
                UUID uuid = reply.getUUID();
                StreamLabels stream = _replies.get(uuid);
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
     * Implementation of LabelStream, translating PnfsLabelMessage replies to a stream
     * of LabelEntries.
     * <p>
     * The stream acts as its own iterator, and multiple iterators are not supported.
     */
    public class StreamLabels
          implements LabelsStream, Iterator<LabelsEntry> {

        private final BlockingQueue<PnfsListLabelsMessage> _queue =
              new LinkedBlockingQueue<>();
        private final UUID _uuid;
        private boolean _isFinal;
        private Iterator<LabelsEntry> _iterator;
        private int _count;
        private int _total;

        public StreamLabels(UUID uuid) {
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
                      "Timeout during label listing.");
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
        public Iterator<LabelsEntry> iterator() {
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
                LOGGER.error("Listing of incomplete: {}", e.getMessage());
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            return true;
        }

        @Override
        public LabelsEntry next() {
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
