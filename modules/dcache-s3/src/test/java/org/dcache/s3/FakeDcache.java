package org.dcache.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.PnfsRenameMessage;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryStream;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsSetFileAttributes;

/**
 * An in-memory namespace with file content, behind the dCache interfaces the S3 door uses:
 * PnfsManager (through a CellStub), directory listing, and reading and writing files.
 * Namespace errors are those of Chimera.
 */
class FakeDcache {

    static final long MTIME = 1_700_000_000_000L;

    final Map<FsPath, FileAttributes> namespace = Collections.synchronizedMap(new HashMap<>());
    final Map<FsPath, byte[]> content = Collections.synchronizedMap(new HashMap<>());
    final Set<FsPath> denied = Collections.synchronizedSet(new HashSet<>());
    final List<FsPath> listed = new CopyOnWriteArrayList<>();
    final List<Subject> requestSubjects = new CopyOnWriteArrayList<>();

    /**
     * Paths written by the writer, in order.
     */
    final List<FsPath> written = new CopyOnWriteArrayList<>();

    /**
     * When set, the writer fails with this error after reading the data.
     */
    volatile CacheException writeError;

    private int nextId;

    /**
     * Adds a directory, and its missing parent directories.
     */
    FileAttributes directory(String path) {
        FsPath parent = FsPath.create(path).parent();
        if (!parent.isRoot() && !namespace.containsKey(parent)) {
            directory(parent.toString());
        }
        FileAttributes attributes = FileAttributes.of()
              .fileType(FileType.DIR)
              .pnfsId(nextPnfsId())
              .creationTime(MTIME)
              .modificationTime(MTIME)
              .build();
        namespace.put(FsPath.create(path), attributes);
        return attributes;
    }

    /**
     * Adds a file with a replica on a pool and no tape copy, and its missing parent directories.
     */
    FileAttributes file(String path, String data) {
        FsPath parent = FsPath.create(path).parent();
        if (!namespace.containsKey(parent)) {
            directory(parent.toString());
        }
        FileAttributes attributes = FileAttributes.of()
              .fileType(FileType.REGULAR)
              .pnfsId(nextPnfsId())
              .size(data.length())
              .creationTime(MTIME)
              .modificationTime(MTIME)
              .locations(List.of("pool1"))
              .storageInfo(new GenericStorageInfo("osm", "test:tape"))
              .build();
        namespace.put(FsPath.create(path), attributes);
        content.put(FsPath.create(path), data.getBytes(UTF_8));
        return attributes;
    }

    /**
     * Moves a file to tape: it gets a tape copy and loses its replicas.
     */
    void onTapeOnly(String path) {
        FileAttributes attributes = namespace.get(FsPath.create(path));
        attributes.getStorageInfo().addLocation(URI.create("osm://osm/?store=test&bfid=1"));
        attributes.setLocations(List.of());
    }

    /**
     * Returns the path of the file or directory with a PNFS-ID.
     */
    Optional<FsPath> pathOf(PnfsId pnfsId) {
        synchronized (namespace) {
            return namespace.entrySet().stream()
                  .filter(e -> e.getValue().getPnfsId().equals(pnfsId))
                  .map(Map.Entry::getKey)
                  .findAny();
        }
    }

    /**
     * A PnfsHandler answering from the namespace.
     */
    PnfsHandler pnfsHandler() {
        CellStub stub = mock(CellStub.class);
        when(stub.send(any(PnfsMessage.class), anyLong())).thenAnswer(i -> {
            PnfsMessage msg = i.getArgument(0);
            assertSerializable(msg);
            requestSubjects.add(msg.getSubject());
            try {
                synchronized (namespace) {
                    if (msg instanceof PnfsGetFileAttributes get) {
                        getFileAttributes(get);
                    } else if (msg instanceof PnfsCreateEntryMessage create) {
                        create(create);
                    } else if (msg instanceof PnfsSetFileAttributes set) {
                        setFileAttributes(set);
                    } else if (msg instanceof PnfsDeleteEntryMessage delete) {
                        delete(delete);
                    } else if (msg instanceof PnfsRenameMessage rename) {
                        rename(rename);
                    } else {
                        throw new AssertionError("Unexpected message " + msg);
                    }
                }
            } catch (CacheException e) {
                msg.setFailed(e.getRc(), e.getMessage());
            }
            return Futures.immediateFuture(msg);
        });
        return new PnfsHandler(stub);
    }

    /**
     * Writes files as the transfer manager does: it creates missing parent directories, and a
     * file that could not be written is removed.  Unlike the files added by the tests, written
     * files and the directories the door creates are new.
     */
    S3ObjectWriter writer() {
        return (user, path, billingPath, xattrs, data, length) -> {
            synchronized (namespace) {
                if (namespace.containsKey(path)) {
                    throw new FileExistsCacheException("File exists");
                }
                createDirectories(path.parent());
            }
            byte[] bytes = readFully(data, length);
            CacheException error = writeError;
            if (error != null) {
                throw error;
            }
            FileAttributes attributes = FileAttributes.of()
                  .fileType(FileType.REGULAR)
                  .pnfsId(nextPnfsId())
                  .size(length)
                  .creationTime(System.currentTimeMillis())
                  .modificationTime(System.currentTimeMillis())
                  .locations(List.of("pool1"))
                  .storageInfo(new GenericStorageInfo("osm", "test:tape"))
                  .xattrs(Map.copyOf(xattrs))
                  .build();
            namespace.put(path, attributes);
            content.put(path, bytes);
            written.add(path);
            return attributes.getPnfsId();
        };
    }

    private static byte[] readFully(InputStream data, long length) throws IOException {
        byte[] bytes = data.readNBytes((int) length);
        if (bytes.length < length) {
            throw new EOFException("Client sent " + bytes.length + " of " + length + " bytes");
        }
        return bytes;
    }

    DirectoryListSource lister() throws Exception {
        DirectoryListSource lister = mock(DirectoryListSource.class);
        when(lister.list(any(), any(), any(), any(), any(), any())).thenAnswer(i -> {
            // ListDirectoryHandler fails on a null range, although the interface allows it.
            requireNonNull(i.getArgument(4), "range");
            // The attributes are sent to PnfsManager.
            assertSerializable(i.getArgument(5));
            return list(i.getArgument(2));
        });
        return lister;
    }

    /**
     * Reads file content from memory.
     */
    S3ObjectReader reader() {
        return (user, path, attributes, offset, length) ->
              new ByteArrayInputStream(content.get(path), (int) offset, (int) length);
    }

    /**
     * Fails like the cell transport does for objects that cannot be serialized.
     */
    static void assertSerializable(Object object) {
        try (var out = new ObjectOutputStream(OutputStream.nullOutputStream())) {
            out.writeObject(object);
        } catch (IOException e) {
            throw new AssertionError("Not serializable: " + object, e);
        }
    }

    private PnfsId nextPnfsId() {
        return new PnfsId(String.format("0000%032X", nextId++));
    }

    private void getFileAttributes(PnfsGetFileAttributes msg) throws CacheException {
        FsPath path = FsPath.create(msg.getPnfsPath());
        FileAttributes attributes = lookup(path);
        checkAllowed(path);
        msg.setFileAttributes(attributes);
    }

    private void create(PnfsCreateEntryMessage msg) throws CacheException {
        FsPath path = FsPath.create(msg.getPnfsPath());
        if (msg.getFileAttributes().getFileType() != FileType.DIR) {
            throw new AssertionError("The door creates files through the writer");
        }
        checkParent(path);
        // Chimera checks permissions before it looks for the name.
        checkAllowed(path.parent());
        if (namespace.containsKey(path)) {
            throw new FileExistsCacheException("File exists: " + path);
        }
        FileAttributes requested = msg.getFileAttributes();
        FileAttributes attributes = directory(path.toString());
        attributes.setCreationTime(System.currentTimeMillis());
        attributes.setModificationTime(System.currentTimeMillis());
        if (requested.isDefined(FileAttribute.XATTR)) {
            attributes.setXattrs(Map.copyOf(requested.getXattrs()));
        }
        msg.setFileAttributes(attributes);
    }

    private void createDirectories(FsPath path) throws CacheException {
        FileAttributes attributes = namespace.get(path);
        if (attributes == null) {
            createDirectories(path.parent());
            checkAllowed(path.parent());
            directory(path.toString());
        } else if (attributes.getFileType() != FileType.DIR) {
            throw new NotDirCacheException("Not a directory");
        }
    }

    private void setFileAttributes(PnfsSetFileAttributes msg) throws CacheException {
        FsPath path = msg.getPnfsPath() == null
              ? pathOf(msg.getPnfsId()).orElseThrow(
                    () -> new FileNotFoundCacheException("No such file: " + msg.getPnfsId()))
              : FsPath.create(msg.getPnfsPath());
        FileAttributes attributes = lookup(path);
        FileAttributes update = msg.getFileAttributes();
        if (update.isDefined(FileAttribute.CHECKSUM)) {
            attributes.addChecksums(update.getChecksums());
        }
        msg.setFileAttributes(attributes);
    }

    private void delete(PnfsDeleteEntryMessage msg) throws CacheException {
        FsPath path = msg.getPnfsPath() == null
              ? pathOf(msg.getPnfsId()).orElseThrow(
                    () -> new FileNotFoundCacheException("No such file: " + msg.getPnfsId()))
              : FsPath.create(msg.getPnfsPath());
        FileAttributes attributes = lookup(path);
        if (msg.getPnfsId() != null && !msg.getPnfsId().equals(attributes.getPnfsId())) {
            throw new FileNotFoundCacheException("No such file: " + msg.getPnfsId());
        }
        if (!msg.getAllowedFileTypes().contains(attributes.getFileType())) {
            if (msg.getAllowedFileTypes().contains(FileType.DIR)) {
                throw new NotDirCacheException("Not a directory");
            }
            throw new NotFileCacheException("Not a file");
        }
        checkAllowed(path);
        if (namespace.keySet().stream().anyMatch(p -> !p.isRoot() && p.parent().equals(path))) {
            throw new CacheException("Directory is not empty: " + path);
        }
        namespace.remove(path);
        content.remove(path);
        msg.setFileAttributes(attributes);
    }

    private void rename(PnfsRenameMessage msg) throws CacheException {
        FsPath source = FsPath.create(msg.getPnfsPath());
        FsPath target = FsPath.create(msg.newName());
        FileAttributes attributes = lookup(source);
        if (msg.getPnfsId() != null && !msg.getPnfsId().equals(attributes.getPnfsId())) {
            throw new FileNotFoundCacheException("No such file: " + msg.getPnfsId());
        }
        checkParent(target);
        checkAllowed(target);
        FileAttributes existing = namespace.get(target);
        if (existing != null) {
            if (!msg.getOverwrite()) {
                throw new FileExistsCacheException("File exists");
            }
            if (existing.getFileType() != attributes.getFileType()) {
                throw new FileExistsCacheException("File exists");
            }
        }
        namespace.remove(source);
        namespace.put(target, attributes);
        byte[] data = content.remove(source);
        if (data != null) {
            content.put(target, data);
        } else {
            content.remove(target);
        }
    }

    /**
     * Returns the attributes of a path, failing as Chimera does if it or a parent is missing.
     */
    private FileAttributes lookup(FsPath path) throws CacheException {
        checkParent(path);
        FileAttributes attributes = namespace.get(path);
        if (attributes == null) {
            throw new FileNotFoundCacheException("No such file or directory: " + path);
        }
        return attributes;
    }

    private void checkParent(FsPath path) throws CacheException {
        for (FsPath parent = path.parent(); !parent.isRoot(); parent = parent.parent()) {
            FileAttributes attributes = namespace.get(parent);
            if (attributes != null && attributes.getFileType() != FileType.DIR) {
                throw new NotDirCacheException("Not a directory");
            }
        }
        if (!path.isRoot() && !namespace.containsKey(path.parent())) {
            throw new FileNotFoundCacheException("No such directory: " + path.parent());
        }
    }

    private void checkAllowed(FsPath path) throws CacheException {
        if (denied.contains(path)) {
            throw new PermissionDeniedCacheException("Permission denied");
        }
    }

    private DirectoryStream list(FsPath directory) throws CacheException {
        listed.add(directory);
        FileAttributes attributes = namespace.get(directory);
        if (attributes == null || attributes.getFileType() != FileType.DIR) {
            throw new FileNotFoundCacheException("No such directory: " + directory);
        }
        List<DirectoryEntry> entries;
        synchronized (namespace) {
            entries = namespace.entrySet().stream()
                  .filter(e -> !e.getKey().isRoot() && e.getKey().parent().equals(directory))
                  .map(e -> new DirectoryEntry(e.getKey().name(), e.getValue()))
                  .collect(Collectors.toList());
        }
        return new DirectoryStream() {
            @Override
            public Iterator<DirectoryEntry> iterator() {
                return entries.iterator();
            }

            @Override
            public void close() {
            }
        };
    }
}
