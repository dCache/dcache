/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.chimera.namespace;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.acl.ACL;
import org.dcache.acl.enums.RsType;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.FsInodeType;
import org.dcache.chimera.StorageLocatable;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.util.Checksum;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

/**
 * A Chimera inode extension that provides easy access to and caching of data
 * associated with an inode.
 *
 * In the Chimera schema, the extended data isn't stored in the inode table
 * and thus isn't accessible through the base class.
 */
public class ExtendedInode extends FsInode
{
    private ImmutableMap<String,byte[]> tags;
    private ImmutableList<Checksum> checksums;
    private ImmutableList<StorageLocatable> locations;
    private ImmutableMap<String, String> flags;
    private Optional<AccessLatency> al;
    private Optional<RetentionPolicy> rp;
    private ACL acl;
    private HashMap<Integer, ExtendedInode> levels;
    private InodeStorageInformation storageInfo;
    private Optional<ExtendedInode> parent;

    public ExtendedInode(ExtendedInode parent, FsInode inode)
    {
        this(inode);
        this.parent = Optional.of(parent);
    }

    public ExtendedInode(FsInode inode)
    {
        this(inode.getFs(), inode.toString());
    }

    public ExtendedInode(FileSystemProvider fs, PnfsId id)
    {
        this(fs, id.toIdString());
    }

    public ExtendedInode(FileSystemProvider fs, String id, FsInodeType type)
    {
        super(fs, id, type);
    }

    public ExtendedInode(FileSystemProvider fs, String id)
    {
        super(fs, id);
    }

    public ExtendedInode(FileSystemProvider fs, String id, int level)
    {
        super(fs, id, level);
    }

    public ExtendedInode(FileSystemProvider fs, String id, FsInodeType type,
                         int level)
    {
        super(fs, id, type, level);
    }

    public ExtendedInode(FileSystemProvider fs)
    {
        super(fs);
    }

    @Override
    public ExtendedInode mkdir(String newDir) throws ChimeraFsException
    {
        return new ExtendedInode(this, super.mkdir(newDir));
    }

    @Override
    public ExtendedInode mkdir(String name, int owner, int group, int mode) throws ChimeraFsException
    {
        return new ExtendedInode(this, super.mkdir(name, owner, group, mode));
    }

    @Override
    public ExtendedInode create(String name, int uid, int gid, int mode) throws ChimeraFsException
    {
        return new ExtendedInode(this, super.create(name, uid, gid, mode));
    }

    @Override
    public ExtendedInode inodeOf(String name) throws ChimeraFsException
    {
        return new ExtendedInode(this, super.inodeOf(name));
    }

    @Override
    public ExtendedInode getParent()
    {
        if (parent == null) {
            FsInode actualParent = super.getParent();
            parent = Optional.fromNullable(actualParent != null ? new ExtendedInode(actualParent) : null);
        }
        return parent.get();
    }

    public ImmutableMap<String,byte[]> getTags() throws ChimeraFsException
    {
        if (tags == null) {
            tags = ImmutableMap.copyOf(_fs.getAllTags(this));
        }
        return tags;
    }

    public ImmutableList<String> getTag(String tag)
            throws IOException
    {
        byte[] data = getTags().get(tag);
        if (data == null || data.length == 0) {
            return ImmutableList.of();
        }
        return ByteSource.wrap(data).asCharSource(Charsets.UTF_8).readLines();
    }

    public ImmutableCollection<Checksum> getChecksums() throws ChimeraFsException
    {
        if (checksums == null) {
            checksums = ImmutableList.copyOf(_fs.getInodeChecksums(this));
        }
        return checksums;
    }

    public ImmutableList<String> getLocations(int type) throws ChimeraFsException
    {
        return ImmutableList.copyOf(
                transform(filter(getLocations(), new IsType(type)), new GetLocation()));
    }

    public ImmutableList<StorageLocatable> getLocations() throws ChimeraFsException
    {
        if (locations == null) {
            locations = ImmutableList.copyOf(_fs.getInodeLocations(this));
        }
        return locations;
    }

    public ImmutableMap<String,String> getFlags() throws ChimeraFsException
    {
        if (flags == null) {
            ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();
            ExtendedInode level2 = getLevel(2);
            try {
                ChimeraCacheInfo info = new ChimeraCacheInfo(level2);
                for (Map.Entry<String,String> e: info.getFlags().entrySet()) {
                    builder.put(e.getKey(), e.getValue());
                }
            } catch (IOException e) {
                throw new ChimeraFsException(e.getMessage(), e);
            }
            flags = builder.build();
        }
        return flags;
    }

    public Optional<AccessLatency> getAccessLatency() throws ChimeraFsException
    {
        if (al == null) {
            al = Optional.fromNullable(_fs.getAccessLatency(this));
        }
        return al;
    }

    public Optional<RetentionPolicy> getRetentionPolicy() throws ChimeraFsException
    {
        if (rp == null) {
            rp = Optional.fromNullable(_fs.getRetentionPolicy(this));
        }
        return rp;
    }

    public ACL getAcl() throws ChimeraFsException
    {
        if (acl == null) {
            RsType rsType = isDirectory() ? RsType.DIR : RsType.FILE;
            acl = new ACL(rsType, _fs.getACL(this));
        }
        return acl;
    }

    public ExtendedInode getLevel(int level)
    {
        if (levels == null) {
            levels = new HashMap<>();
        }
        ExtendedInode inode = levels.get(level);
        if (inode == null) {
            inode = new ExtendedInode(_fs, toString(), level);
            levels.put(level, inode);
        }
        return inode;
    }

    public InodeStorageInformation getStorageInfo() throws ChimeraFsException
    {
        if (storageInfo == null) {
            storageInfo = _fs.getStorageInfo(this);
        }
        return storageInfo;
    }

    public FsPath getPath() throws ChimeraFsException
    {
        return new FsPath(_fs.inode2path(this));
    }

    private static class IsType implements Predicate<StorageLocatable>
    {
        private final int type;

        public IsType(int type)
        {
            this.type = type;
        }

        @Override
        public boolean apply(StorageLocatable location)
        {
            return location.type() == type;
        }
    }

    private static class GetLocation implements Function<StorageLocatable, String>
    {
        @Override
        public String apply(StorageLocatable location)
        {
            return location.location();
        }
    }
}
