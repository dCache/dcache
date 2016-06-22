package org.dcache.pool.repository;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diskCacheV111.util.PnfsId;

/**
 * A file store layout keeping all files in a single subdirectory
 * called "data".
 */
public class FlatFileStore implements FileStore
{
    private final Path _dataDir;

    public FlatFileStore(Path baseDir) throws IOException
    {
        if (!Files.isDirectory(baseDir)) {
            throw new FileNotFoundException("No such directory: " + baseDir);
        }

        _dataDir = baseDir.resolve("data");
        if (!Files.exists(_dataDir)) {
            Files.createDirectory(_dataDir);
        } else if (!Files.isDirectory(_dataDir)) {
            throw new FileNotFoundException("No such directory: " + _dataDir);
        }
    }

    /**
     * Returns a human readable description of the file store.
     */
    public String toString()
    {
        return _dataDir.toString();
    }

    @Override
    public Path get(PnfsId id)
    {
        return _dataDir.resolve(id.toString());
    }

    @Override
    public Set<PnfsId> index() throws IOException
    {
        try (Stream<Path> files = Files.list(_dataDir)) {
            return files
                    .map(p -> p.getFileName().toString())
                    .filter(PnfsId::isValid)
                    .map(PnfsId::new)
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public long getFreeSpace() throws IOException
    {
        return Files.getFileStore(_dataDir).getUsableSpace();
    }

    @Override
    public long getTotalSpace() throws IOException
    {
        return Files.getFileStore(_dataDir).getTotalSpace();
    }

    @Override
    public boolean isOk()
    {
        try {
            Path tmp = _dataDir.resolve(".repository_is_ok");
            Files.deleteIfExists(tmp);
            Files.createFile(tmp);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
