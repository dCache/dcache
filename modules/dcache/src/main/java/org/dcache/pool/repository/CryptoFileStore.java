package org.dcache.pool.repository;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.net.URI;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diskCacheV111.util.PnfsId;
import org.cryptomator.cryptofs.CryptoFileSystem;
import org.cryptomator.cryptofs.CryptoFileSystemProperties;
import org.cryptomator.cryptofs.CryptoFileSystemProvider;


/**
 * A file store layout keeping all files in a single subdirectory
 * called "data".
 */
public class CryptoFileStore implements FileStore {

    private final static String PASS = "password";

    private final Path dataDir;
    private final Path root;
    private final CryptoFileSystem fileSystem;

    public CryptoFileStore(Path baseDir) throws IOException {

        if (!Files.isDirectory(baseDir)) {
            throw new FileNotFoundException("No such directory: " + baseDir);
        }

        dataDir = baseDir.resolve("data");
        if (!Files.exists(dataDir)) {
            Files.createDirectory(dataDir);
            CryptoFileSystemProvider.initialize(dataDir, "masterkey.cryptomator", PASS);
        } else if (!Files.isDirectory(dataDir)) {
            throw new FileNotFoundException("No such directory: " + dataDir);
        }

        fileSystem = CryptoFileSystemProvider.newFileSystem(
                dataDir,
                CryptoFileSystemProperties.cryptoFileSystemProperties()
                        .withPassphrase(PASS)
                        .build());

        root = fileSystem.getPath("/");
    }

    public void close() throws IOException {
        fileSystem.close();
    }

    /**
     * Returns a human readable description of the file store.
     */
    public String toString() {
        return dataDir.toString();
    }

    private Path getPath(PnfsId id) {
        return root.resolve(id.toString());
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
    public RepositoryChannel openDataChannel(PnfsId id, Set<? extends OpenOption> mode) throws IOException {
        return new FileRepositoryChannel(getPath(id), mode);
    }

    @Override
    public void remove(PnfsId id) throws IOException {
        Files.deleteIfExists(getPath(id));
    }

    @Override
    public Set<PnfsId> index() throws IOException {
        try (Stream<Path> files = Files.list(dataDir)) {
            return files
                    .map(p -> p.getFileName().toString())
                    .filter(PnfsId::isValid)
                    .map(PnfsId::new)
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public long getFreeSpace() throws IOException {
        return Files.getFileStore(dataDir).getUsableSpace();
    }

    @Override
    public long getTotalSpace() throws IOException {
        return Files.getFileStore(dataDir).getTotalSpace();
    }

    @Override
    public boolean isOk() {
        try {
            Path tmp = dataDir.resolve(".repository_is_ok");
            Files.deleteIfExists(tmp);
            Files.createFile(tmp);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
