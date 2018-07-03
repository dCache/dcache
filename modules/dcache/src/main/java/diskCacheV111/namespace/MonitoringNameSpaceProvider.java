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

import com.google.common.collect.Range;
import com.google.common.io.BaseEncoding;
import org.apache.curator.shaded.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import org.dcache.namespace.events.EventType;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;

import org.dcache.auth.Subjects;
import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

import static java.util.Objects.requireNonNull;


/**
 * An implementation of NameSpaceProvider that wraps some other instance of
 * NameSpaceProvider and sends inotify-like events.
 */
public class MonitoringNameSpaceProvider extends ForwardingNameSpaceProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringNameSpaceProvider.class);

    private static final EnumSet<FileAttribute> PNFSID = EnumSet.of(FileAttribute.PNFSID);
    private static final EnumSet<FileAttribute> FILETYPE = EnumSet.of(FileAttribute.TYPE);
    private static final EnumSet<FileAttribute> PNFSID_FILETYPE = EnumSet.of(FileAttribute.PNFSID, FileAttribute.TYPE);

    private NameSpaceProvider delegate;
    private EventReceiver eventReceiver;

    @Required
    public void setNameSpaceProvider(NameSpaceProvider namespace)
    {
        delegate = requireNonNull(namespace);
    }

    @Required
    public void setEventReceiver(EventReceiver receiver)
    {
        eventReceiver = requireNonNull(receiver);
    }

    @Override
    protected NameSpaceProvider delegate()
    {
        return delegate;
    }

    private Optional<Link> findParent(String path)
    {
        try {
            FsPath fsPath = FsPath.create(path);
            PnfsId parent = super.pathToPnfsid(Subjects.ROOT, fsPath.parent().toString(), true);
            return Optional.of(new Link(parent, fsPath.name()));
        } catch (FileNotFoundCacheException e) {
            // Don't log, just don't notify.
        } catch (CacheException e) {
            LOGGER.error("Failed to resolve path {}: {}", path, e.toString());
        }
        return Optional.empty();
    }

    private Collection<Link> find(String path)
    {
        try {
            return find(super.pathToPnfsid(Subjects.ROOT, path, true));
        } catch (FileNotFoundCacheException e) {
            // Don't log, just don't notify.
        } catch (CacheException e) {
            LOGGER.warn("Failed to find path {}: {}", path, e.toString());
        }
        return Collections.emptyList();
    }

    private Collection<Link> find(PnfsId target)
    {
        try {
            return super.find(Subjects.ROOT, target);
        } catch (FileNotFoundCacheException e) {
            // Don't log, just don't notify.
        } catch (CacheException e) {
            LOGGER.warn("Failed to find target {}: {}", target, e.toString());
        }
        return Collections.emptyList();
    }

    private static Set<FileAttribute> union(Set<FileAttribute> first, Set<FileAttribute> second)
    {
        if (first.containsAll(second)) {
            return first;
        }

        if (second.containsAll(first)) {
            return second;
        }

        EnumSet<FileAttribute> combined = EnumSet.copyOf(first);
        combined.addAll(second);
        return combined;
    }

    private void notifyParents(PnfsId target, EventType event, FileType type)
    {
        find(target).forEach(l -> eventReceiver.notifyChildEvent(event,
                l.getParent(), l.getName(), type));
    }

    @Override
    public FileAttributes setFileAttributes(Subject subject, PnfsId target,
            FileAttributes attr, Set<FileAttribute> fetch) throws CacheException
    {
        FileAttributes returnAttr = super.setFileAttributes(subject, target, attr,
                union(fetch,FILETYPE));

        FileType type = returnAttr.getFileType();

        eventReceiver.notifySelfEvent(EventType.IN_ATTRIB, target, type);
        notifyParents(target, EventType.IN_ATTRIB, type);

        return returnAttr;
    }

    @Override
    public FileAttributes createFile(Subject subject, String path,
            FileAttributes assignAttributes, Set<FileAttribute> requestAttributes)
            throws CacheException
    {
        FileAttributes ret = super.createFile(subject, path, assignAttributes,
                union(requestAttributes, PNFSID));

        notifyParents(ret.getPnfsId(), EventType.IN_CREATE, FileType.REGULAR);

        return ret;
    }

    @Override
    public PnfsId createDirectory(Subject subject, String path,
            FileAttributes attributes) throws CacheException
    {
        PnfsId id = super.createDirectory(subject, path, attributes);

        notifyParents(id, EventType.IN_CREATE, FileType.DIR);

        return id;
    }

    /**
     * A ListHandler implementation that wraps some other ListHandler and
     * instruments the directory listing for events.
     */
    private class MonitoringListHandler implements ListHandler
    {
        private final ListHandler inner;
        private final PnfsId target;
        private final Collection<Link> links;
        private boolean sentOpen;

        public MonitoringListHandler(ListHandler inner, PnfsId target, Collection<Link> links)
        {
            this.inner = inner;
            this.target = target;
            this.links = links;
        }

        private void notify(EventType type)
        {
            if (target != null) {
                eventReceiver.notifySelfEvent(type, target, FileType.DIR);
            }
            links.forEach(l -> eventReceiver.notifyChildEvent(type,
                    l.getParent(), l.getName(), FileType.DIR));
        }

        private void notifyOpen()
        {
            if (!sentOpen) {
                notify(EventType.IN_OPEN);
                sentOpen = true;
            }
        }

        @Override
        public void addEntry(String name, FileAttributes attrs) throws CacheException
        {
            /*
             * To avoid sending OPEN event if there is a problem with the
             * request (e.g., user does not have permission to read the
             * directory, path isn't a directory) the OPEN event is sent only
             * when the first directory item is provided.
             */
            notifyOpen();
            inner.addEntry(name, attrs);
        }

        public void sendClose()
        {
            notifyOpen();
            notify(EventType.IN_CLOSE_NOWRITE);
        }
    }

    @Override
    public void list(Subject subject, String path, Glob glob, Range<Integer> range,
            Set<FileAttribute> attrs, ListHandler handler) throws CacheException
    {
        PnfsId target;
        Collection<Link> links;
        try {
            target = super.pathToPnfsid(Subjects.ROOT, path, true);
            links = find(target);
        } catch (CacheException e) {
            target = null;
            links = Collections.emptyList();
        }

        MonitoringListHandler monitoringHandler = new MonitoringListHandler(handler, target, links);
        super.list(subject, path, glob, range, attrs, monitoringHandler);
        monitoringHandler.sendClose();
    }


    @Override
    public PnfsId createSymLink(Subject subject, String path, String dest,
            FileAttributes attributes) throws CacheException
    {
        PnfsId id = super.createSymLink(subject, path, dest, attributes);

        notifyParents(id, EventType.IN_CREATE, FileType.LINK);

        return id;
    }

    @Override
    public void rename(Subject subject, PnfsId target, String sourcePath,
            String destinationPath, boolean overwrite) throws CacheException
    {
        Optional<Link> sourceParent = findParent(sourcePath);

        PnfsId resolvedTarget = null;
        try {
            resolvedTarget = target == null ? super.pathToPnfsid(Subjects.ROOT, sourcePath, true) : target;
        } catch (FileNotFoundCacheException e) {
        } catch (CacheException e) {
            LOGGER.error("Failed to resolve path {}: {}", sourcePath, e.toString());
        }

        super.rename(subject, target, sourcePath, destinationPath, overwrite);

        if (resolvedTarget == null) {
            LOGGER.error("Earlier failure in monitoring lookup didn't fail move.");
            return;
        }

        // The cookie is a unique identifier for this move operation.
        byte[] cookieBytes = Hashing.murmur3_128().newHasher()
                .putString(resolvedTarget.toString(), StandardCharsets.UTF_8)
                .putString(sourcePath, StandardCharsets.UTF_8)
                .putString(destinationPath, StandardCharsets.UTF_8)
                .putLong(System.currentTimeMillis()) // FIXME replace with generation ID
                .hash().asBytes();
        String cookie = BaseEncoding.base64().omitPadding().encode(cookieBytes);

        Optional<Link> destinationParent = findParent(destinationPath);

        FileType type = super.getFileAttributes(Subjects.ROOT, resolvedTarget, FILETYPE).getFileType();
        eventReceiver.notifySelfEvent(EventType.IN_MOVE_SELF, resolvedTarget, type);
        sourceParent.ifPresent(l -> eventReceiver.notifyMovedEvent(EventType.IN_MOVED_FROM,
                l.getParent(), l.getName(), cookie, type));
        destinationParent.ifPresent(l -> eventReceiver.notifyMovedEvent(EventType.IN_MOVED_TO,
                l.getParent(), l.getName(), cookie, type));
    }


    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
            PnfsId target, Set<FileAttribute> attr) throws CacheException
    {
        Collection<Link> locations = find(target);

        FileAttributes attributes = super.deleteEntry(subject, allowed, target,
                union(attr, FILETYPE));

        eventReceiver.notifySelfEvent(EventType.IN_DELETE_SELF, target, attributes.getFileType());
        locations.forEach(l -> eventReceiver.notifyChildEvent(EventType.IN_DELETE,
                l.getParent(), l.getName(), attributes.getFileType()));

        return attributes;
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
            String path, Set<FileAttribute> attr) throws CacheException
    {
        Collection<Link> links = find(path);

        FileAttributes attributes = super.deleteEntry(subject, allowed, path,
                union(attr, PNFSID_FILETYPE));

        eventReceiver.notifySelfEvent(EventType.IN_DELETE_SELF,
                attributes.getPnfsId(), attributes.getFileType());
        links.forEach(l -> eventReceiver.notifyChildEvent(EventType.IN_DELETE,
                l.getParent(), l.getName(), attributes.getFileType()));

        return attributes;
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
            PnfsId target, String path, Set<FileAttribute> attr)
            throws CacheException
    {
        Collection<Link> links = find(target);

        FileAttributes attributes = super.deleteEntry(subject, allowed,
                target, path, union(attr, FILETYPE));

        eventReceiver.notifySelfEvent(EventType.IN_DELETE_SELF, target, attributes.getFileType());
        links.forEach(l -> eventReceiver.notifyChildEvent(EventType.IN_DELETE,
                l.getParent(), l.getName(), attributes.getFileType()));

        return attributes;
    }

    @Override
    public void addCacheLocation(Subject subject, PnfsId target,
            String cacheLocation) throws CacheException
    {
        Collection<Link> links = find(target);

        super.addCacheLocation(subject, target, cacheLocation);

        eventReceiver.notifySelfEvent(EventType.IN_ATTRIB, target, FileType.REGULAR);
        links.forEach(l -> eventReceiver.notifyChildEvent(EventType.IN_ATTRIB,
                l.getParent(), l.getName(), FileType.REGULAR));
    }

    @Override
    public void clearCacheLocation(Subject subject, PnfsId target,
            String cacheLocation, boolean removeIfLast) throws CacheException
    {
        Collection<Link> links = find(target);

        super.clearCacheLocation(subject, target, cacheLocation, removeIfLast);

        eventReceiver.notifySelfEvent(EventType.IN_ATTRIB, target, FileType.REGULAR);
        links.forEach(l -> eventReceiver.notifyChildEvent(EventType.IN_ATTRIB,
                l.getParent(), l.getName(), FileType.REGULAR));
    }

    @Override
    public FileAttributes commitUpload(Subject subject, FsPath uploadPath,
            FsPath path, Set<CreateOption> options, Set<FileAttribute> fetch)
            throws CacheException
    {
        FileAttributes attr = super.commitUpload(subject, uploadPath, path, options, union(fetch, PNFSID));

        byte[] cookieBytes = Hashing.murmur3_128().newHasher()
                .putString(attr.getPnfsId().toString(), StandardCharsets.UTF_8)
                .putString(uploadPath.toString(), StandardCharsets.UTF_8)
                .putString(path.toString(), StandardCharsets.UTF_8)
                .putLong(System.currentTimeMillis()) // FIXME replace with generation ID
                .hash().asBytes();
        String cookie = BaseEncoding.base64().omitPadding().encode(cookieBytes);

        find(attr.getPnfsId()).forEach(l ->
                eventReceiver.notifyMovedEvent(EventType.IN_MOVED_TO, l.getParent(),
                        l.getName(), cookie, FileType.REGULAR));
        return attr;
    }
}
