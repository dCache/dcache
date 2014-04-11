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
package org.dcache.pool.nearline.tar;

import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.InvalidMessageCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;

/**
 * This class is for demonstration only. The code is a bit messy and incomplete.
 */
public class TarNearlineStorage implements NearlineStorage
{
    private final String type;
    private final String name;

    private final BlockingQueue<FlushRequest> flushQueue = new LinkedBlockingDeque<>();
    private final Multimap<String,StageRequest> stageRequests =
            Multimaps.synchronizedMultimap(ArrayListMultimap.<String,StageRequest>create());

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile File directory;

    public TarNearlineStorage(String type, String name)
    {
        this.type = type;
        this.name = name;
    }

    protected Iterable<URI> getLocations(FileAttributes fileAttributes)
    {
        return filter(fileAttributes.getStorageInfo().locations(),
                      new Predicate<URI>()
                      {
                          @Override
                          public boolean apply(URI uri)
                          {
                              return uri.getScheme().equals(type) && uri.getAuthority().equals(name);
                          }
                      });
    }

    @Override
    public void flush(Iterable<FlushRequest> requests)
    {
        Iterables.addAll(flushQueue, requests);
        executor.execute(new FlushTask());
    }

    @Override
    public void stage(Iterable<StageRequest> requests)
    {
        for (StageRequest request : requests) {
            File tarFile;
            try {
                FileAttributes fileAttributes = request.getFileAttributes();
                URI location = getFirst(getLocations(fileAttributes), null);
                if (location == null) {
                    throw new CacheException(CacheException.BROKEN_ON_TAPE, "File not on nearline storage: " + fileAttributes.getPnfsId());
                }
                String path = location.getPath();
                if (path == null) {
                    throw new InvalidMessageCacheException("Invalid nearline storage URI: " + location);
                }
                tarFile = new File(path).getParentFile();
                if (tarFile == null) {
                    throw new InvalidMessageCacheException("Invalid nearline storage URI: " + location);
                }
            } catch (CacheException e) {
                request.failed(e);
                continue;
            }
            stageRequests.put(tarFile.getName(), request);
        }
        executor.execute(new StageTask());
    }

    @Override
    public void remove(Iterable<RemoveRequest> requests)
    {
        for (RemoveRequest request : requests) {
            request.failed(new CacheException("Remove from tar nearline storage is not supported."));
        }
    }

    @Override
    public void cancel(UUID uuid)
    {
        // Not implemented
    }

    @Override
    public void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        String directory = properties.get("directory");
        checkArgument(directory != null, "directory attribute is required");
        this.directory = new File(directory);
    }

    @Override
    public void shutdown()
    {
        executor.shutdownNow();
    }

    private class FlushTask implements Runnable
    {
        @Override
        public void run()
        {
            List<FlushRequest> requests = new ArrayList<>();
            flushQueue.drainTo(requests);
            if (!requests.isEmpty()) {
                Map<File, URI> uris = new HashMap<>();
                String tarName = UUID.randomUUID().toString();
                File tarFile = new File(directory, tarName + ".tar");
                try (FileOutputStream out = new FileOutputStream(tarFile)) {
                    try (TarArchiveOutputStream tarStream = new TarArchiveOutputStream(out)) {
                        for (FlushRequest request : requests) {
                            request.activate().get();
                            File file = request.getFile();
                            PnfsId pnfsId = request.getFileAttributes().getPnfsId();
                            TarArchiveEntry entry = new TarArchiveEntry(pnfsId.getId());
                            entry.setSize(request.getFile().length());
                            tarStream.putArchiveEntry(entry);
                            Files.copy(file.toPath(), tarStream);
                            tarStream.closeArchiveEntry();
                            uris.put(file, new URI(type, name, '/' + tarName + '/' + pnfsId.getId(), null, null));
                        }
                        tarStream.finish();
                    }
                } catch (Exception e) {
                    try {
                        Files.deleteIfExists(tarFile.toPath());
                    } catch (IOException suppressed) {
                        e.addSuppressed(suppressed);
                    }
                    for (FlushRequest request : requests) {
                        request.failed(e);
                    }
                    return;
                }
                for (FlushRequest request : requests) {
                    request.completed(Collections.singleton(uris.get(request.getFile())));
                }
            }
        }
    }

    private class StageTask implements Runnable
    {
        @Override
        public void run()
        {
            List<String> archives;
            synchronized(stageRequests) {
                archives = new ArrayList<>(stageRequests.keySet());
            }
            for (String archive : archives) {
                Map<String, StageRequest> requests = new HashMap<>();
                for (StageRequest request : stageRequests.removeAll(archive)) {
                    try {
                        request.activate().get();
                        requests.put(request.getFileAttributes().getPnfsId().getId(), request);
                    } catch (Exception e) {
                        request.failed(e);
                    }
                }

                File tarFile = new File(directory, archive + ".tar");
                try (FileInputStream in = new FileInputStream(tarFile)) {
                    try (TarArchiveInputStream tarStream = new TarArchiveInputStream(in)) {
                        TarArchiveEntry entry;
                        while (!requests.isEmpty() && (entry = tarStream.getNextTarEntry()) != null) {
                            StageRequest request = requests.remove(entry.getName());
                            if (request != null) {
                                try {
                                    request.allocate().get();
                                    Files.copy(tarStream, request.getFile().toPath());
                                    request.completed(Collections.<Checksum>emptySet());
                                } catch (Exception e) {
                                    request.failed(e);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    for (StageRequest request : requests.values()) {
                        request.failed(e);
                    }
                }

                for (StageRequest request : requests.values()) {
                    request.failed(new CacheException(CacheException.BROKEN_ON_TAPE, "File not found: " + request.getFileAttributes().getPnfsId()));
                }
            }
        }
    }
}
