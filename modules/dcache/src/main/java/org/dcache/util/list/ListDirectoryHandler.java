package org.dcache.util.list;

import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;

/**
 * DirectoryListSource which delegates the list operation to the
 * PnfsManager.
 *
 * Large directories are broken into several reply messages by the
 * PnfsManager. For that reason the regular Cells callback mechanism
 * for replies cannot be used. Instead messages of type
 * PnfsListDirectoryMessage must be routed to the
 * ListDirectoryHandler. This also has the consequence that a
 * ListDirectoryHandler cannot be used from the Cells messages
 * thread. Any attempt to do so will cause the message thread to
 * block, as the replies cannot be delivered to the
 * ListDirectoryHandler.
 */
public class ListDirectoryHandler
    implements CellMessageReceiver, DirectoryListSource
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(ListDirectoryHandler.class);

    private final PnfsHandler _pnfs;
    private final Map<UUID,Stream> _replies =
            new ConcurrentHashMap<>();

    public ListDirectoryHandler(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
    }

    /**
     * Sends a directory list request to PnfsManager. The result is
     * provided as a stream of directory entries.
     * <p>
     * The method blocks until the first set of directory entries have
     * been received from the server.  Hence errors like
     * FILE_NOT_FOUND are thrown by the call to the list method rather
     * than while iterating over the stream.
     * <p>
     * Note that supplied subject and restriction values will be overwritten if
     * {@link PnfsHandler#setSubject} or {@link PnfsHandler#setRestriction} have
     * been called on the underlying PnfsHandler instance.
     */
    @Override
    public DirectoryStream
        list(Subject subject, Restriction restriction, FsPath path, Glob pattern, Range<Integer> range)
        throws InterruptedException, CacheException
    {
        return list(subject, restriction, path, pattern, range,
                    EnumSet.noneOf(FileAttribute.class));
    }

    /**
     * Sends a directory list request to PnfsManager. The result is
     * provided as a stream of directory entries.
     * <p>
     * The method blocks until the first set of directory entries have
     * been received from the server.  Hence errors like
     * FILE_NOT_FOUND are thrown by the call to the list method rather
     * than while iterating over the stream.
     * <p>
     * Note that supplied subject and restriction values will be overwritten if
     * {@link PnfsHandler#setSubject} or {@link PnfsHandler#setRestriction} have
     * been called on the underlying PnfsHandler instance.
     */
    @Override
    public DirectoryStream
        list(Subject subject, Restriction restriction, FsPath path, Glob pattern,
                Range<Integer> range, Set<FileAttribute> attributes)
                throws InterruptedException, CacheException
    {
        String dir = path.toString();
        PnfsListDirectoryMessage msg =
            new PnfsListDirectoryMessage(dir, pattern, range, attributes);
        UUID uuid = msg.getUUID();
        boolean success = false;
        Stream stream = new Stream(dir, uuid);
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

    @Override
    public void printFile(Subject subject, Restriction restriction,
            DirectoryListPrinter printer, FsPath path)
            throws InterruptedException, CacheException
    {
        PnfsHandler handler = new PnfsHandler(_pnfs, subject, restriction);
        Set<FileAttribute> required = printer.getRequiredAttributes();
        FileAttributes attributes = handler.getFileAttributes(path.toString(), required);
        DirectoryEntry entry = new DirectoryEntry(path.name(), attributes);
        if (path.isRoot()) {
            printer.print(null, null, entry);
        } else {
            FileAttributes dirAttr = handler.getFileAttributes(path.parent().toString(), required);
            printer.print(path.parent(), dirAttr, entry);
        }
        printer.close();
    }

    @Override
    public int printDirectory(Subject subject, Restriction restriction,
            DirectoryListPrinter printer, FsPath path, Glob glob, Range<Integer> range)
            throws InterruptedException, CacheException
    {
        Set<FileAttribute> required =
            printer.getRequiredAttributes();
        FileAttributes dirAttr =
            _pnfs.getFileAttributes(path.toString(), required);
        try (DirectoryStream stream = list(subject, restriction, path, glob, range, required)) {
            int total = 0;
            for (DirectoryEntry entry: stream) {
                printer.print(path, dirAttr, entry);
                total++;
            }
            printer.close();
            return total;
        }
    }

    /**
     * Callback for delivery of replies from
     * PnfsManager. PnfsListDirectoryMessage have to be routed to this
     * message.
     */
    public void messageArrived(PnfsListDirectoryMessage reply)
    {
        if (reply.isReply()) {
            try {
                UUID uuid = reply.getUUID();
                Stream stream = _replies.get(uuid);
                if (stream != null) {
                    stream.put(reply);
                } else {
                    LOGGER.warn("Received list result for an unknown request. Directory listing was possibly incomplete.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Implementation of DirectoryStream, translating
     * PnfsListDirectoryMessage replies to a stream of
     * DirectoryEntries.
     *
     * The stream acts as its own iterator, and multiple iterators are
     * not supported.
     */
    public class Stream
        implements DirectoryStream, Iterator<DirectoryEntry>
    {
        private final BlockingQueue<PnfsListDirectoryMessage> _queue =
                new LinkedBlockingQueue<>();
        private final UUID _uuid;
        private final String _path;
        private boolean _isFinal;
        private Iterator<DirectoryEntry> _iterator;
        private int _count;
        private int _total;

        public Stream(String path, UUID uuid)
        {
            _path = path;
            _uuid = uuid;
        }

        @Override
        public void close()
        {
            _replies.remove(_uuid);
        }

        private void put(PnfsListDirectoryMessage msg)
            throws InterruptedException
        {
            _queue.put(msg);
        }

        private void waitForMoreEntries()
            throws InterruptedException, CacheException
        {
            if (_isFinal) {
                _iterator = null;
                return;
            }

            PnfsListDirectoryMessage msg =
                _queue.poll(_pnfs.getPnfsTimeout(), TimeUnit.MILLISECONDS);
            if (msg == null) {
                throw new CacheException(CacheException.TIMEOUT,
                                         "Timeout during directory listing.");
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
        public Iterator<DirectoryEntry> iterator()
        {
            return this;
        }

        @Override
        public boolean hasNext()
        {
            try {
                if (_iterator == null || !_iterator.hasNext()) {
                    waitForMoreEntries();
                    if (_iterator == null) {
                        return false;
                    }
                }
            } catch (CacheException e) {
                LOGGER.error("Listing of {} incomplete: {}", _path, e.getMessage());
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            return true;
        }

        @Override
        public DirectoryEntry next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return _iterator.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

}
