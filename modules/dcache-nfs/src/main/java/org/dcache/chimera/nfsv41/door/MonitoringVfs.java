/*
 * Copyright (c) 2018 Deutsches Elektronen-Synchroton,
 * Member of the Helmholtz Association, (DESY), HAMBURG, GERMANY
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera.nfsv41.door;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diskCacheV111.namespace.EventReceiver;
import diskCacheV111.util.PnfsId;

import org.dcache.nfs.vfs.ForwardingFileSystem;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.namespace.FileType;
import org.dcache.nfs.status.BadHandleException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.Stat.Type;

import static java.util.Objects.requireNonNull;
import static org.dcache.namespace.events.EventType.*;

/**
 * A VirtualFileSystem that forwards requests to some other VirtualFileSystem
 * while adds support for monitoring activity and sending inotify-like  events.
 */
public class MonitoringVfs extends ForwardingFileSystem
{
    /**
     * REVISIT: is this a copy from NameSpaceProvider?
     */
    public class Link
    {
        private final PnfsId parent;
        private final String name;
        public Link(PnfsId parent, String name)
        {
            this.parent = parent;
            this.name = name;
        }
        public PnfsId getParent()
        {
            return parent;
        }
        public String getName()
        {
            return name;
        }
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringVfs.class);

    private VirtualFileSystem inner;
    private FileSystemProvider fs;
    private EventReceiver eventReceiver;

    @Required
    public void setInner(VirtualFileSystem fileSystem)
    {
        inner = requireNonNull(fileSystem);
    }

    @Required
    public void setFileSystemProvider(FileSystemProvider provider)
    {
        fs = requireNonNull(provider);
    }

    @Required
    public void setEventReceiver(EventReceiver receiver)
    {
        eventReceiver = receiver;
    }

    @Override
    protected VirtualFileSystem delegate()
    {
        return inner;
    }

    private Collection<Link> findInNamespace(Inode target)
    {
        /*
         * NB. this returns all links of a file/directory.  If a file has
         * multiple hard links then we notify the parent of all links.  This
         * behaviour is different from inotify, which notifies only the parent
         * of the link that was modified, despite the action affecting all
         * links.
         */
        Optional<Collection<Link>> result = toFsInode(target).flatMap(inode -> {
                    try {
                        return Optional.of(fs.find(inode).stream()
                                .map(l -> toPnfsId(l.getParent())
                                            .map(p -> new Link(p, l.getName()))
                                            .orElse(null))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()));
                    } catch (ChimeraFsException e) {
                        LOGGER.error("findInNamespace failed for {}: {}", target, e.toString());
                        return Optional.empty();
                    }
                });
        return result.orElse(Collections.emptyList());
    }

    private Optional<FsInode> toFsInode(Inode target)
    {
        try {
            return Optional.of(ChimeraVfs.inodeFromBytes(fs, target.getFileId()));
        } catch (BadHandleException e) {
            LOGGER.warn("Bad handle {}: {}", target, e.toString());
            return Optional.empty();
        }
    }

    private Optional<PnfsId> toPnfsId(Inode target)
    {
        return toFsInode(target).flatMap(MonitoringVfs::toPnfsId);
    }

    private static Optional<PnfsId> toPnfsId(FsInode target)
    {
        try {
            return Optional.of(new PnfsId(target.getId()));
        } catch (ChimeraFsException e) {
            LOGGER.warn("Problem converting inode {} to PnfsId: {}", target, e.toString());
            return Optional.empty();
        }
    }

    private static FileType asFileType(Type type)
    {
        switch (type) {
        case REGULAR:
            return FileType.REGULAR;
        case DIRECTORY:
            return FileType.DIR;
        case SYMLINK:
            return FileType.LINK;
        default:
            return FileType.SPECIAL;
        }
    }

    @Override
    public Inode mkdir(Inode parent, String name, Subject subject, int mode)
            throws IOException
    {
        Inode dir = super.mkdir(parent, name, subject, mode);

        toPnfsId(parent).ifPresent(id -> eventReceiver.notifyChildEvent(IN_CREATE,
                id, name, FileType.DIR));
        return dir;
    }


    @Override
    public Inode create(Inode parent, Stat.Type type, String name,
            Subject subject, int mode) throws IOException
    {
        Inode target = super.create(parent, type, name, subject, mode);

        toPnfsId(parent).ifPresent(id -> eventReceiver.notifyChildEvent(IN_CREATE,
                id, name, asFileType(type)));

        return target;
    }


    @Override
    public Inode link(Inode parent, Inode link, String name, Subject subject)
            throws IOException
    {
        Inode newLink = super.link(parent, link, name, subject);

        toPnfsId(parent).ifPresent(id -> eventReceiver.notifyChildEvent(IN_CREATE,
                id, name, FileType.REGULAR));

        return newLink;
    }

    @Override
    public Inode symlink(Inode parent, String name, String target, Subject subject,
            int mode) throws IOException
    {
        Inode symlink = super.symlink(parent, name, target, subject, mode);

        toPnfsId(parent).ifPresent(id -> eventReceiver.notifyChildEvent(IN_CREATE,
                id, name, FileType.LINK));

        return symlink;
    }


    @Override
    public void setattr(Inode inode, Stat stat) throws IOException
    {
        super.setattr(inode, stat);

        FileType type = asFileType(super.getattr(inode).type());
        toPnfsId(inode).ifPresent(id -> eventReceiver.notifySelfEvent(IN_ATTRIB,
                id, type));
        findInNamespace(inode).forEach(l -> eventReceiver.notifyChildEvent(IN_ATTRIB,
                l.getParent(), l.getName(), type));
    }


    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException
    {
        super.setAcl(inode, acl);

        FileType type = asFileType(super.getattr(inode).type());
        toPnfsId(inode).ifPresent(id -> eventReceiver.notifySelfEvent(IN_ATTRIB,
                id, type));
        findInNamespace(inode).forEach(l -> eventReceiver.notifyChildEvent(IN_ATTRIB,
                l.getParent(), l.getName(), type));
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count)
            throws IOException
    {
        int result = super.read(inode, data, offset, count);

        if (result > 0) {

            /* REVISIT:
             * NFS supports reading data from a file without first opening it.
             * This does not conform to POSIX (and therefore inotify) semantics,
             * making the choice of events to send something of a compromise.
             * Here, we generate synthetic IN_OPEN and IN_CLOSE_NOWRITE events,
             * which yields a series of events that is correct, but may be
             * excessive: multiple successive read operations will generate
             * unnecessary (IN_CLOSE_NOWRITE, IN_OPEN) events being sent.
             *
             * Future work could add heuristics to suppress these by (for example)
             * delaying the IN_CLOSE_NOWRITE to see if another read operation
             * is attempted.
             */

            toPnfsId(inode).ifPresent(id -> {
                        eventReceiver.notifySelfEvent(IN_OPEN, id, FileType.REGULAR);
                        eventReceiver.notifySelfEvent(IN_ACCESS, id, FileType.REGULAR);
                        eventReceiver.notifySelfEvent(IN_CLOSE_NOWRITE, id, FileType.REGULAR);
                    });
            findInNamespace(inode).forEach(l -> {
                        eventReceiver.notifyChildEvent(IN_OPEN, l.getParent(), l.getName(), FileType.REGULAR);
                        eventReceiver.notifyChildEvent(IN_ACCESS, l.getParent(), l.getName(), FileType.REGULAR);
                        eventReceiver.notifyChildEvent(IN_CLOSE_NOWRITE, l.getParent(), l.getName(), FileType.REGULAR);
                    });
        }

        return result;
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count,
            StabilityLevel stabilityLevel) throws IOException
    {

        WriteResult result = super.write(inode, data, offset, count, stabilityLevel);

        if (result.getBytesWritten() > 0) {

            /* REVISIT:
             * NFS supports writing data to a file without first opening it.
             * This does not conform to POSIX (and therefore inotify) semantics,
             * making the choice of events to send something of a compromise.
             * Here, we generate synthetic IN_OPEN and IN_CLOSE_WRITE events,
             * which yields a series of events that is correct, but may be
             * excessive: multiple successive write operations will generate
             * unnecessary (IN_CLOSE_WRITE, IN_OPEN) events being sent.
             *
             * Future work could add heuristics to suppress these by (for example)
             * delaying the IN_CLOSE_WRITE to see if another write operation
             * is attempted.
             */

            toPnfsId(inode).ifPresent(id -> {
                        eventReceiver.notifySelfEvent(IN_OPEN, id, FileType.REGULAR);
                        eventReceiver.notifySelfEvent(IN_MODIFY, id, FileType.REGULAR);
                        eventReceiver.notifySelfEvent(IN_CLOSE_WRITE, id, FileType.REGULAR);
                    });
            findInNamespace(inode).forEach(l -> {
                        eventReceiver.notifyChildEvent(IN_OPEN, l.getParent(), l.getName(), FileType.REGULAR);
                        eventReceiver.notifyChildEvent(IN_MODIFY, l.getParent(), l.getName(), FileType.REGULAR);
                        eventReceiver.notifyChildEvent(IN_CLOSE_WRITE, l.getParent(), l.getName(), FileType.REGULAR);
                    });
        }

        return result;
    }


    @Override
    public void remove(Inode parent, String name) throws IOException {
        Optional<PnfsId> parentId = toPnfsId(parent);

        Optional<PnfsId> target;
        FileType type = FileType.SPECIAL;
        try {
            Inode targetInode = super.lookup(parent, name);
            type = asFileType(super.getattr(targetInode).type());
            target = toPnfsId(targetInode);
        } catch (NoEntException ignored) {
            target = Optional.empty(); // don't send event.
            parentId = Optional.empty(); // don't send event.
        } catch (IOException e) {
            LOGGER.warn("Problem looking up {} in inode {}: {}", name, parent, e.toString());
            target = Optional.empty(); // don't send event.
            parentId = Optional.empty(); // don't send event.
        }

        /* REVISIT: update VirtualFileSystem#remove to return attributes of delete item? */
        super.remove(parent, name);

        FileType t = type;
        parentId.ifPresent(id -> eventReceiver.notifyChildEvent(IN_DELETE, id, name, t));
        target.ifPresent(id -> eventReceiver.notifySelfEvent(IN_DELETE_SELF, id, t));
    }


    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName)
            throws IOException
    {
        boolean changed = super.move(src, oldName, dest, newName);

        if (changed) {
            try {
                Inode targetInode = super.lookup(dest, newName);
                FileType type = asFileType(super.getattr(targetInode).type());
                long generation = super.getattr(dest).getGeneration();
                Optional<PnfsId> srcParent = toPnfsId(src);
                Optional<PnfsId> destParent = toPnfsId(dest);
                Optional<PnfsId> target = toPnfsId(targetInode);

                // The cookie is a unique identifier for this move operation.  It
                // is completely unrelated to the NFS-protocol cookie for
                // directory listing
                Hasher hasher = Hashing.murmur3_128().newHasher().putLong(generation);
                destParent.ifPresent(id -> hasher.putString(id.toString(), StandardCharsets.UTF_8));
                String cookie = BaseEncoding.base64().omitPadding().encode(hasher.hash().asBytes());

                srcParent.ifPresent(id -> eventReceiver.notifyMovedEvent(IN_MOVED_FROM,
                        id, oldName, cookie, type));
                destParent.ifPresent(id -> eventReceiver.notifyMovedEvent(IN_MOVED_TO,
                        id, newName, cookie, type));
                target.ifPresent(id -> eventReceiver.notifySelfEvent(IN_MOVE_SELF,
                        id, type));
            } catch (IOException e) {
                LOGGER.error("Failed establishing move of {} to {} in dir {}: {}",
                        oldName, newName, dest, e.toString());
            }
        }

        return changed;
    }

    @Override
    public DirectoryStream list(Inode inode, byte[] verifier, long cookie)
            throws IOException
    {
        DirectoryStream stream = super.list(inode, verifier, cookie);

        // Here we assume that a cookie of 0 corresponds to a new directory
        // listing, so should generate the IN_OPEN event on the directory.
        if (cookie == 0) {
            toPnfsId(inode).ifPresent(id -> eventReceiver.notifySelfEvent(IN_OPEN,
                    id, FileType.DIR));
            findInNamespace(inode).forEach(l -> eventReceiver.notifyChildEvent(IN_OPEN,
                    l.getParent(), l.getName(), FileType.DIR));

            // There is (currently) no equivalent to closedir(3) for directory
            // listing in the NFS protocol.  We could generate a IN_CLOSE_NOWRITE
            // when the last item is supplied; however, there's no guarantee
            // that the client will read all items.  Therefore, we issue a
            // synthetic IN_CLOSE_NOWRITE immediately after the IN_OPEN event.
            toPnfsId(inode).ifPresent(id -> eventReceiver.notifySelfEvent(IN_CLOSE_NOWRITE,
                    id, FileType.DIR));
            findInNamespace(inode).forEach(l -> eventReceiver.notifyChildEvent(IN_CLOSE_NOWRITE,
                    l.getParent(), l.getName(), FileType.DIR));
        }

        return stream;
    }
}
