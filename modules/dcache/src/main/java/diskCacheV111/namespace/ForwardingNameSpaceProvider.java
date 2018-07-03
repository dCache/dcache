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

import javax.security.auth.Subject;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

/**
 * An implementation of NameSpaceProvider that forwards all requests
 * to some other NameSpaceProvider.
 */
public abstract class ForwardingNameSpaceProvider implements NameSpaceProvider
{
    protected abstract NameSpaceProvider delegate();

    @Override
    public FileAttributes createFile(Subject subject, String path,
            FileAttributes assignAttributes, Set<FileAttribute> requestAttributes)
            throws CacheException
    {
        return delegate().createFile(subject, path, assignAttributes,
                requestAttributes);
    }

    @Override
    public PnfsId createDirectory(Subject subject, String path,
            FileAttributes attributes) throws CacheException
    {
        return delegate().createDirectory(subject, path, attributes);
    }

    @Override
    public PnfsId createSymLink(Subject subject, String path, String dest,
            FileAttributes attributes) throws CacheException
    {
        return delegate().createSymLink(subject, path, dest, attributes);
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
            PnfsId pnfsId, Set<FileAttribute> attr) throws CacheException
    {
        return delegate().deleteEntry(subject, allowed, pnfsId, attr);
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
            String path, Set<FileAttribute> attr) throws CacheException
    {
        return delegate().deleteEntry(subject, allowed, path, attr);
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
            PnfsId pnfsId, String path, Set<FileAttribute> attr)
            throws CacheException
    {
        return delegate().deleteEntry(subject, allowed, pnfsId, path, attr);
    }

    @Override
    public void rename(Subject subject, PnfsId pnfsId, String sourcePath,
            String destinationPath, boolean overwrite) throws CacheException
    {
        delegate().rename(subject, pnfsId, sourcePath, destinationPath, overwrite);
    }

    @Override
    public String pnfsidToPath(Subject subject, PnfsId pnfsId)
            throws CacheException
    {
        return delegate().pnfsidToPath(subject, pnfsId);
    }

    @Override
    public PnfsId pathToPnfsid(Subject subject, String path,
            boolean followLinks) throws CacheException
    {
        return delegate().pathToPnfsid(subject, path, followLinks);
    }

    @Override
    public Collection<Link> find(Subject subject, PnfsId pnfsId)
            throws CacheException
    {
        return delegate().find(subject, pnfsId);
    }

    @Override
    public void removeFileAttribute(Subject subject, PnfsId pnfsId,
            String attribute) throws CacheException
    {
        delegate().removeFileAttribute(subject, pnfsId, attribute);
    }

    @Override
    public void removeChecksum(Subject subject, PnfsId pnfsId,
            ChecksumType type) throws CacheException
    {
        delegate().removeChecksum(subject, pnfsId, type);
    }

    @Override
    public void addCacheLocation(Subject subject, PnfsId pnfsId,
            String cacheLocation) throws CacheException
    {
        delegate().addCacheLocation(subject, pnfsId, cacheLocation);
    }

    @Override
    public List<String> getCacheLocation(Subject subject, PnfsId pnfsId)
            throws CacheException
    {
        return delegate().getCacheLocation(subject, pnfsId);
    }

    @Override
    public void clearCacheLocation(Subject subject, PnfsId pnfsId,
            String cacheLocation, boolean removeIfLast) throws CacheException
    {
        delegate().clearCacheLocation(subject, pnfsId, cacheLocation, removeIfLast);
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
            Set<FileAttribute> attr) throws CacheException
    {
        return delegate().getFileAttributes(subject, pnfsId, attr);
    }

    @Override
    public FileAttributes setFileAttributes(Subject subject, PnfsId pnfsId,
            FileAttributes attr, Set<FileAttribute> fetch) throws CacheException
    {
        return delegate().setFileAttributes(subject, pnfsId, attr, fetch);
    }

    @Override
    public void list(Subject subject, String path, Glob glob, Range<Integer> range,
            Set<FileAttribute> attrs, ListHandler handler) throws CacheException
    {
        delegate().list(subject, path, glob, range, attrs, handler);
    }

    @Override
    public FsPath createUploadPath(Subject subject, FsPath path, FsPath rootPath,
            Long size, AccessLatency al, RetentionPolicy rp, String spaceToken,
            Set<CreateOption> options) throws CacheException
    {
        return delegate().createUploadPath(subject, path, rootPath, size, al,
                rp, spaceToken, options);
    }

    @Override
    public FileAttributes commitUpload(Subject subject, FsPath uploadPath,
            FsPath path, Set<CreateOption> options, Set<FileAttribute> fetch)
            throws CacheException
    {
        return delegate().commitUpload(subject, uploadPath, path, options, fetch);
    }

    @Override
    public Collection<FileAttributes> cancelUpload(Subject subject,
            FsPath uploadPath, FsPath path, Set<FileAttribute> attr,
            String explanation) throws CacheException
    {
        return delegate().cancelUpload(subject, uploadPath, path, attr, explanation);
    }
}
