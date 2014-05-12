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
package org.dcache.pool.nearline.script;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.HsmRunSystem;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.pool.nearline.AbstractBlockingNearlineStorage;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.BoundedExecutor;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static java.util.Arrays.asList;

/**
 * NearlineStorage implementation doing callouts to an HSM integration script.
 *
 * This implementation provides backwards compatibility with the legacy HSM scripts that
 * used to be the only HSM integration in dCache.
 */
public class ScriptNearlineStorage extends AbstractBlockingNearlineStorage
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ScriptNearlineStorage.class);

    private static final int MAX_LINES = 200;
    public static final String COMMAND = "command";
    public static final String CONCURRENT_PUTS = "c:puts";
    public static final String CONCURRENT_GETS = "c:gets";
    public static final String CONCURRENT_REMOVES = "c:removes";
    private static final int DEFAULT_FLUSH_THREADS = 100;
    private static final int DEFAULT_STAGE_THREADS = 100;
    private static final int DEFAULT_REMOVE_THREADS = 1;
    private static final Collection<String> PROPERTIES = asList(COMMAND, CONCURRENT_PUTS, CONCURRENT_GETS, CONCURRENT_REMOVES);

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final CDCExecutorServiceDecorator<BoundedExecutor> flushExecutor =
            new CDCExecutorServiceDecorator<>(new BoundedExecutor(executor, DEFAULT_FLUSH_THREADS));
    private final CDCExecutorServiceDecorator<BoundedExecutor> stageExecutor =
            new CDCExecutorServiceDecorator<>(new BoundedExecutor(executor, DEFAULT_STAGE_THREADS));
    private final CDCExecutorServiceDecorator<BoundedExecutor> removeExecutor =
            new CDCExecutorServiceDecorator<>(new BoundedExecutor(executor, DEFAULT_REMOVE_THREADS));

    private volatile String command;
    private volatile String options;

    public ScriptNearlineStorage(String type, String name)
    {
        super(type, name);
    }

    @Override
    protected Executor getFlushExecutor()
    {
        return flushExecutor;
    }

    @Override
    protected Executor getStageExecutor()
    {
        return stageExecutor;
    }

    @Override
    protected Executor getRemoveExecutor()
    {
        return removeExecutor;
    }

    @Override
    protected Set<URI> flush(FlushRequest request) throws IOException, CacheException
    {
        try {
            Set<URI> locations = new HashSet<>();
            String storeCommand = getFlushCommand(request.getFile(), request.getFileAttributes());
            String output = new HsmRunSystem(storeCommand, MAX_LINES, request.getDeadline() - System.currentTimeMillis()).execute();
            for (String uri : Splitter.on("\n").trimResults().omitEmptyStrings().split(output)) {
                try {
                    locations.add(new URI(uri));
                } catch (URISyntaxException e) {
                    throw new CacheException(2, "HSM script produced bad URI: " + e.getMessage(), e);
                }
            }
            return locations;
        } catch (IllegalThreadStateException e) {
            throw new CacheException(3, e.getMessage(), e);
        }
    }

    @Override
    protected Set<Checksum> stage(StageRequest request) throws IOException, CacheException
    {
        try {
            FileAttributes attributes = request.getFileAttributes();
            String fetchCommand = getFetchCommand(request.getFile(), attributes);
            new HsmRunSystem(fetchCommand, MAX_LINES, request.getDeadline() - System.currentTimeMillis()).execute();
            return readChecksumFromHsm(request.getFile());
        } catch (IllegalThreadStateException  e) {
            throw new CacheException(3, e.getMessage(), e);
        }
    }

    @Override
    protected void remove(RemoveRequest request) throws IOException, CacheException
    {
        new HsmRunSystem(getRemoveCommand(request.getUri()), MAX_LINES, request.getDeadline() - System.currentTimeMillis()).execute();
    }

    @Override
    public synchronized void configure(Map<String, String> properties)
    {
        if (!properties.containsKey(COMMAND)) {
            throw new IllegalArgumentException("command option must be defined");
        }
        command = buildCommand(properties);
        options = buildOptions(properties);

        configureThreadPoolSize(flushExecutor.delegate(), properties.get(CONCURRENT_PUTS), 1);
        configureThreadPoolSize(stageExecutor.delegate(), properties.get(CONCURRENT_GETS), 1);
        configureThreadPoolSize(removeExecutor.delegate(), properties.get(CONCURRENT_REMOVES), 1);
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        flushExecutor.shutdown();
        stageExecutor.shutdown();
        removeExecutor.shutdown();
        executor.shutdown();
    }

    private String getFlushCommand(File file, FileAttributes fileAttributes)
    {
        StorageInfo storageInfo = StorageInfos.extractFrom(fileAttributes);
        StringBuilder sb = new StringBuilder(command);
        sb.append(" put ").
                append(fileAttributes.getPnfsId()).append(' ').
                append(file.getPath());
        sb.append(" -si=").append(storageInfo.toString());
        sb.append(options);
        LOGGER.debug("COMMAND: {}", sb);
        return sb.toString();
    }

    private String getFetchCommand(File file, FileAttributes attributes)
    {
        StorageInfo storageInfo = StorageInfos.extractFrom(attributes);
        StringBuilder sb = new StringBuilder(command);
        sb.append(" get ").
                append(attributes.getPnfsId()).append(' ').
                append(file.getPath());
        sb.append(" -si=").append(storageInfo.toString());
        for (URI location: getLocations(attributes)) {
            if (location.getScheme().equals(type) && location.getAuthority().equals(name)) {
                sb.append(" -uri=").append(location.toString());
            }
        }
        sb.append(options);
        LOGGER.debug("COMMAND: {}", sb);
        return sb.toString();
    }

    private String getRemoveCommand(URI uri)
    {
        return command + " -uri=" + uri + " remove";
    }

    private Set<Checksum> readChecksumFromHsm(File file)
            throws IOException
    {
        File checksumFile = new File(file.getCanonicalPath() + ".crcval");
        try {
            if (checksumFile.exists()) {
                try {
                    String firstLine = Files.readFirstLine(checksumFile, Charsets.US_ASCII);
                    if (firstLine != null) {
                        Checksum checksum = Checksum.parseChecksum("1:" + firstLine);
                        return Collections.singleton(checksum);
                    }
                } finally {
                    checksumFile.delete();
                }
            }
        } catch (FileNotFoundException e) {
                /* Should not happen unless somebody else is removing
                 * the file before we got a chance to read it.
                 */
            throw Throwables.propagate(e);
        }
        return Collections.emptySet();
    }

    private void configureThreadPoolSize(BoundedExecutor executor, String configuration, int defaultValue)
    {
        int n = (configuration != null) ? Integer.parseInt(configuration) : defaultValue;
        executor.setMaximumPoolSize(n);
    }

    private String buildCommand(Map<String, String> properties)
    {
        return properties.get(COMMAND);
    }

    private String buildOptions(Map<String, String> properties)
    {
        StringBuilder options = new StringBuilder();
        for (Map.Entry<String,String> attr : Maps.filterKeys(properties, not(in(PROPERTIES))).entrySet()) {
            String key = attr.getKey();
            String val = attr.getValue();
            options.append(" -").append(key);
            if (!Strings.isNullOrEmpty(val)) {
                options.append('=').append(val);
            }
        }
        return options.toString();

    }
}
