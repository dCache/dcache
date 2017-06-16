package org.dcache.pool.repository.meta.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import diskCacheV111.util.AccessLatency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Collection;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;
import java.net.URISyntaxException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.Document;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;

import org.dcache.vehicles.FileAttributes;

public class CacheRepositoryEntryImpl implements ReplicaRecord, ReplicaRecord.UpdatableRecord {


    private final static int FORMAT_VERSION = 1;

    /**
     * Update options to do an upsert.
     */
    private static final UpdateOptions UPSERT = new UpdateOptions()
            .upsert(true)
            .bypassDocumentValidation(true); // update aka add new fields

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheRepositoryEntryImpl.class);
    private final CacheRepositoryEntryState state;
    private final PnfsId pnfsId;
    private int linkCount;
    private StorageInfo storageInfo;
    private long creationTime = System.currentTimeMillis();
    private long lastAccess;
    private long size;

    /**
     * File store used for data file.
     */
    private final FileStore fileStore;

    private final MongoCollection<Document> collection;

    private final Document dbKey;

    public CacheRepositoryEntryImpl(String pool, PnfsId pnfsId, MongoCollection<Document> collection, FileStore fileStore) throws IOException {

        this.pnfsId = pnfsId;
        this.collection = collection;
        this.fileStore = fileStore;

        dbKey = new Document()
                .append("pool", pool)
                .append("pnfsid", pnfsId.toIdString());

        state = new CacheRepositoryEntryState(dbKey, collection);

        load();

        try {
            BasicFileAttributes attributes = fileStore
                    .getFileAttributeView(pnfsId)
                    .readAttributes();
            lastAccess = attributes.lastModifiedTime().toMillis();
            size = attributes.size();
        } catch (FileNotFoundException | NoSuchFileException fnf) {
            /*
             * it's not an error state.
             */
        }

        if (lastAccess == 0) {
            lastAccess = creationTime;
        }
    }

    @Override
    public synchronized int incrementLinkCount() {
        linkCount++;
        return linkCount;
    }

    @Override
    public synchronized int decrementLinkCount() {

        if (linkCount <= 0) {
            throw new IllegalStateException("Link count is already  zero");
        }
        linkCount--;
        return linkCount;
    }

    @Override
    public synchronized int getLinkCount() {
        return linkCount;
    }

    @Override
    public synchronized long getCreationTime() {
        return creationTime;
    }

    @Override
    public synchronized URI getReplicaUri() {
        return fileStore.get(pnfsId);
    }

    @Override
    public RepositoryChannel openChannel(Set<StandardOpenOption> mode) throws IOException {
        return fileStore.openDataChannel(pnfsId, mode);
    }

    @Override
    public synchronized long getLastAccessTime() {
        return lastAccess;
    }

    @Override
    public synchronized void setLastAccessTime(long time) throws CacheException {
        try {
            fileStore
                    .getFileAttributeView(pnfsId)
                    .setTimes(FileTime.fromMillis(time), null, null);
        } catch (IOException e) {
            throw new DiskErrorCacheException("Failed to set modification time: " + pnfsId, e);
        }
        lastAccess = System.currentTimeMillis();
    }

    @Override
    public synchronized PnfsId getPnfsId() {
        return pnfsId;
    }

    @Override
    public synchronized long getReplicaSize() {
        try {
            return state.getState().isMutable()
                    ? fileStore.getFileAttributeView(pnfsId).readAttributes().size()
                    : size;
        } catch (NoSuchFileException e) {
            return 0;
        } catch (IOException e) {
            LOGGER.error("Failed to read file size: " + e);
            return 0;
        }
    }

    @Override
    public synchronized boolean isSticky() {
        return state.isSticky();
    }

    @Override
    public synchronized ReplicaState getState() {
        return state.getState();
    }

    @Override
    public synchronized Void setState(ReplicaState newState)
            throws CacheException {
        try {
            if (state.getState().isMutable() && !newState.isMutable()) {
                try {
                    size = fileStore.getFileAttributeView(pnfsId).readAttributes().size();
                } catch (NoSuchFileException e) {
                    size = 0;
                }
            }
            state.setState(newState);
        } catch (IOException e) {
            throw new DiskErrorCacheException(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public synchronized Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException {
        try {
            return state.removeExpiredStickyFlags();
        } catch (IOException e) {
            throw new DiskErrorCacheException(e.getMessage(), e);
        }
    }

    @Override
    public boolean setSticky(String owner, long expire, boolean overwrite) throws CacheException {
        try {
            return state.setSticky(owner, expire, overwrite);

        } catch (IllegalStateException | IOException e) {
            throw new DiskErrorCacheException(e.getMessage(), e);
        }
    }

    @Override
    public synchronized FileAttributes getFileAttributes() {
        FileAttributes attributes = FileAttributes.ofPnfsId(pnfsId);
        StorageInfo si = getStorageInfo();
        if (si != null) {
            StorageInfos.injectInto(si, attributes);
        }
        return attributes;
    }

    @Override
    public Void setFileAttributes(FileAttributes attributes) throws CacheException {
        try {
            if (attributes.isDefined(FileAttribute.STORAGEINFO)) {
                setStorageInfo(StorageInfos.extractFrom(attributes));
            } else {
                setStorageInfo(null);
            }
        } catch (IOException e) {
            throw new DiskErrorCacheException(pnfsId + " " + e.getMessage(), e);
        }
        return null;
    }

    private synchronized StorageInfo getStorageInfo() {
        return storageInfo;
    }

    private synchronized void setStorageInfo(StorageInfo si) throws IOException {

        Map<String, ? extends Object> m = si.getMap();
        List<String> locations = si.locations().stream().map(Object::toString).collect(Collectors.toList());

        Document siDoc = new Document(dbKey)
                .append("version", FORMAT_VERSION)
                .append("created", creationTime)
                .append("hsm", si.getHsm())
                .append("storageClass", si.getStorageClass())
                .append("size", si.getLegacySize())
                .append("accessLatency", si.getLegacyAccessLatency().toString())
                .append("retentionPolicy", si.getLegacyRetentionPolicy().toString())
                .append("locations", locations)
                .append("map", new Document((Map<String, Object>) m));

        collection.updateOne(dbKey, new Document("$set", siDoc), UPSERT);

        this.storageInfo = si;
    }

    private void load() throws IOException {

        FindIterable<Document> res = collection.find(dbKey);
        Document d = res.first();
        if (d == null) {
            // storage info not stored yet
            return;
        }

        if (!d.containsKey("version")) {
            // not fully writen record
            return;
        }

        int version = d.getInteger("version");
        if (version > FORMAT_VERSION) {
            LOGGER.error("unsupported version");
            throw new IOException("unsupported metadata version");
        }
        storageInfo = new GenericStorageInfo(d.getString("hsm"), d.getString("storageClass"));

        storageInfo.setLegacyAccessLatency(AccessLatency.valueOf(d.getString("accessLatency")));
        storageInfo.setLegacyRetentionPolicy(RetentionPolicy.valueOf(d.getString("retentionPolicy")));
        storageInfo.setLegacySize(d.getLong("size"));

        @SuppressWarnings("unchecked")
        ArrayList<String> locations = d.get("locations", ArrayList.class);
        locations.forEach(l -> {
            try {
                storageInfo.addLocation(new URI(l));
            } catch (URISyntaxException e) {
                LOGGER.warn("malformed location URL: {} : {}", l, e.getMessage());
            }
        });

        Document keymap = d.get("map", Document.class);
        keymap.keySet()
                .forEach((k) -> {
                    storageInfo.setKey(k, keymap.getString(k));
                });
        creationTime = d.getLong("created");
    }

    @Override
    public synchronized Collection<StickyRecord> stickyRecords() {
        return state.stickyRecords();
    }

    @Override
    public synchronized <T> T update(Update<T> update) throws CacheException {
        return update.apply(this);
    }
}
