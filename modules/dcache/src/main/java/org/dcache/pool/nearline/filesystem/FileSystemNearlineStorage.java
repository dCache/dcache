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
package org.dcache.pool.nearline.filesystem;

import com.google.common.collect.Iterables;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.InvalidMessageCacheException;

import org.dcache.pool.nearline.AbstractBlockingNearlineStorage;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class FileSystemNearlineStorage extends AbstractBlockingNearlineStorage
{
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private Path directory;

    public FileSystemNearlineStorage(String type, String name)
    {
        super(type, name);
    }

    /**
     * Returns a path to the external storage for the give name.
     */
    private Path getExternalPath(String name)
    {
        return directory.resolve(name);
    }

    @Override
    protected Executor getFlushExecutor()
    {
        return executor;
    }

    @Override
    protected Executor getStageExecutor()
    {
        return executor;
    }

    @Override
    protected Executor getRemoveExecutor()
    {
        return executor;
    }

    @Override
    protected Set<URI> flush(FlushRequest request) throws IOException, URISyntaxException
    {
        File file = request.getFile();
        flush(file.toPath(), getExternalPath(file.getName()));
        URI uri = new URI(type, name, '/' + request.getFileAttributes().getPnfsId().getId(), null, null);
        return Collections.singleton(uri);
    }

    @Override
    protected Set<Checksum> stage(StageRequest request) throws CacheException, IOException
    {
        FileAttributes fileAttributes = request.getFileAttributes();
        URI location = Iterables.getFirst(getLocations(fileAttributes), null);
        if (location == null) {
            throw new CacheException(CacheException.BROKEN_ON_TAPE, "File not on nearline storage: " + fileAttributes.getPnfsId());
        }
        String path = location.getPath();
        if (path == null) {
            throw new InvalidMessageCacheException("Invalid nearline storage URI: " + location);
        }
        stage(getExternalPath(path.substring(1)), request.getFile().toPath());
        return Collections.emptySet();
    }

    @Override
    public void remove(RemoveRequest request) throws IOException, InvalidMessageCacheException
    {
        String path = request.getUri().getPath();
        if (path == null) {
            throw new InvalidMessageCacheException("Invalid nearline storage URI: " + request.getUri());
        }
        remove(getExternalPath(path.substring(1)));
    }

    @Override
    public void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        String directory = properties.get("directory");
        checkArgument(directory != null, "directory attribute is required");
        this.directory = FileSystems.getDefault().getPath(directory);
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        executor.shutdown();
    }

    protected abstract void flush(Path path, Path externalPath) throws IOException;
    protected abstract void stage(Path externalPath, Path path) throws IOException;
    protected abstract void remove(Path externalPath) throws IOException;
}
