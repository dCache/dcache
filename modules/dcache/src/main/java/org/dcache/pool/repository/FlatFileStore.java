package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * A file store layout keeping all files in a single subdirectory called "data".
 */
public class FlatFileStore implements FileStore {

    private final Path _dataDir;

    public FlatFileStore(Path baseDir) throws IOException {
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
    public String toString() {
        return _dataDir.toString();
    }

    private Path getPath(PnfsId id) {
        return _dataDir.resolve(id.toString());
    }

    @Override
    public URI get(PnfsId id) {
        return getPath(id).toUri();
    }

    @Override
    public boolean contains(PnfsId id) {
        return Files.exists(getPath(id));
    }

    @Override
    public BasicFileAttributeView getFileAttributeView(PnfsId id) {
        Path p = getPath(id);
        return Files.getFileAttributeView(p, BasicFileAttributeView.class);
    }

    @Override
    public URI create(PnfsId id) throws IOException {
        Path p = getPath(id);
        Files.createFile(p);
        return p.toUri();
    }

    @Override
    public RepositoryChannel openDataChannel(PnfsId id, Set<? extends OpenOption> mode)
          throws IOException {
        return new FileRepositoryChannel(getPath(id), mode);
    }

    @Override
    public void remove(PnfsId id) throws IOException {
        Files.deleteIfExists(getPath(id));
    }

    @Override
    public Set<PnfsId> index() throws IOException {
        try (Stream<Path> files = Files.list(_dataDir)) {
            return files
                  .map(p -> p.getFileName().toString())
                  .filter(PnfsId::isValid)
                  .map(PnfsId::new)
                  .collect(Collectors.toSet());
        }
    }

    @Override
    public long getFreeSpace() throws IOException {
        return Files.getFileStore(_dataDir).getUsableSpace();
    }

    @Override
    public long getTotalSpace() throws IOException {
        return Files.getFileStore(_dataDir).getTotalSpace();
    }

    @Override
    public FileStoreState isOk() {
        try {
            Path tmp = _dataDir.resolve(".repository_is_ok");
            Files.deleteIfExists(tmp);
            Files.createFile(tmp);
            return FileStoreState.OK;
        } catch (IOException e) {
            return FileStoreState.FAILED;
        }
    }
}
