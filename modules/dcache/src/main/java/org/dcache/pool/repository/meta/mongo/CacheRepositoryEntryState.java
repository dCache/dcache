package org.dcache.pool.repository.meta.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;

import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.entry.state.Sticky;

public class CacheRepositoryEntryState {

    /**
     * Update options to do an upsert.
     */
    private static final UpdateOptions UPSERT = new UpdateOptions()
            .upsert(true)
            .bypassDocumentValidation(true); // update aka add new fields

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheRepositoryEntryState.class);

    // possible states of entry in the repository
    private final Sticky _sticky = new Sticky();
    private ReplicaState _state;

    private final MongoCollection<Document> collection;
    private final Document dbKey;

    public CacheRepositoryEntryState(Document dbKey, MongoCollection<Document> collection) throws IOException {
        this.dbKey = dbKey;
        this.collection = collection;
        _state = ReplicaState.NEW;

        loadState();
    }

    public List<StickyRecord> removeExpiredStickyFlags() throws IOException {
        List<StickyRecord> removed = _sticky.removeExpired();
        if (!removed.isEmpty()) {
            makeStatePersistent();
        }
        return removed;
    }

    public void setState(ReplicaState state)
            throws IOException {
        if (state != _state) {
            _state = state;
            makeStatePersistent();
        }
    }

    public ReplicaState getState() {
        return _state;
    }

    /*
     *
     *  State transitions
     *
     */
    public boolean setSticky(String owner, long expire, boolean overwrite)
            throws IllegalStateException, IOException {
        if (_state == ReplicaState.REMOVED || _state == ReplicaState.DESTROYED) {
            throw new IllegalStateException("Entry in removed state");
        }

        // if sticky flag modified, make changes persistent
        if (_sticky.addRecord(owner, expire, overwrite)) {
            makeStatePersistent();
            return true;
        }
        return false;
    }

    public boolean isSticky() {
        return _sticky.isSet();
    }

    /**
     * store state in control file
     *
     * @throws IOException
     */
    private void makeStatePersistent() throws IOException {

        Map<String, Object> stickyRecords = new HashMap<>();
        _sticky.records().stream()
                .forEach(r -> stickyRecords.put(r.owner(), r.expire()));

        Document d = new Document(dbKey)
                .append("replicaState", _state.name())
                .append("stickyRecords", new Document(stickyRecords));

        collection.updateOne(dbKey, new Document("$set", d), UPSERT);
    }

    private void loadState() throws IOException {

        FindIterable<Document> res = collection.find(dbKey);

        Document d = res.first();
        if (d == null) {
            return;
        }

        _state = ReplicaState.BROKEN;
        String state = d.getString("replicaState");
        if (state != null) {
            _state = ReplicaState.valueOf(state);
        }

        Document stickyRecords = d.get("stickyRecords", Document.class);

        if (stickyRecords != null) {
            stickyRecords.entrySet().forEach(e -> {
                _sticky.addRecord(e.getKey(), (Long)e.getValue(), true);
            });
        }
    }

    public Collection<StickyRecord> stickyRecords() {
        return _sticky.records();
    }
}
