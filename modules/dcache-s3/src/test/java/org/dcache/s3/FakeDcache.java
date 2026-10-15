package org.dcache.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.GenericStorageInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryStream;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

/**
 * An in-memory namespace with file content, behind the dCache interfaces the S3 door uses:
 * PnfsManager (through a CellStub), directory listing, file locality and file reading.
 */
class FakeDcache {

    static final long MTIME = 1_700_000_000_000L;

    final Map<FsPath, FileAttributes> namespace = Collections.synchronizedMap(new HashMap<>());
    final Map<FsPath, byte[]> content = Collections.synchronizedMap(new HashMap<>());
    final Set<FsPath> denied = Collections.synchronizedSet(new HashSet<>());
    final List<FsPath> listed = new CopyOnWriteArrayList<>();
    final List<Subject> requestSubjects = new CopyOnWriteArrayList<>();

    private int nextId;

    FileAttributes directory(String path) {
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
     * Adds a file with a replica on a pool and no tape copy.
     */
    FileAttributes file(String path, String data) {
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
     * A PnfsHandler answering PnfsGetFileAttributes from the namespace.
     */
    PnfsHandler pnfsHandler() {
        CellStub stub = mock(CellStub.class);
        when(stub.send(any(PnfsGetFileAttributes.class), anyLong())).thenAnswer(i -> {
            PnfsGetFileAttributes msg = i.getArgument(0);
            assertSerializable(msg);
            requestSubjects.add(msg.getSubject());
            getFileAttributes(msg);
            return Futures.immediateFuture(msg);
        });
        return new PnfsHandler(stub);
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

    private void getFileAttributes(PnfsGetFileAttributes msg) {
        FsPath path = FsPath.create(msg.getPnfsPath());
        for (FsPath parent = path.parent(); !parent.isRoot(); parent = parent.parent()) {
            FileAttributes attributes = namespace.get(parent);
            if (attributes != null && attributes.getFileType() != FileType.DIR) {
                msg.setFailed(CacheException.NOT_DIR, "Not a directory");
                return;
            }
        }
        FileAttributes attributes = namespace.get(path);
        if (attributes == null) {
            msg.setFailed(CacheException.FILE_NOT_FOUND, "No such file or directory");
        } else if (denied.contains(path)) {
            msg.setFailed(CacheException.PERMISSION_DENIED, "Permission denied");
        } else {
            msg.setFileAttributes(attributes);
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
