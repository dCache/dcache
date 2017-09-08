package org.dcache.pool.repository.meta.mongo;

import com.google.common.base.Stopwatch;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;

import java.util.Map;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.EnvironmentAware;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.bson.Document;

import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.ReplicaStore;
import org.dcache.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link ReplicaStore} implementation back-ended with MongoDB.
 *
 * The generated document looks like:
 * <pre>
 * {
 *     "_id" : ObjectId("58e644fc3a5f31198a1c7164"),
 *     "pnfsid" : "000054AD15829F6E4CBB84D17E3DDAFA3915",
 *     "pool" : "dcache-lab001-A",
 *     "version" : 1,
 *     "created" : NumberLong("1491485562656"),
 *     "hsm" : "osm",
 *     "storageClass" : "generated:data",
 *     "size" : NumberLong(438),
 *     "accessLatency" : "NEARLINE",
 *     "retentionPolicy" : "CUSTODIAL",
 *     "locations" : [ ],
 *     "map" : {
 *     	"uid" : "3750",
 *     	"gid" : "3750",
 *     	"flag-c" : "1:ea6f9470"
 *     },
 *     "replicaState" : "PRECIOUS",
 *     "stickyRecords" : {
 *
 *     }
 * }
 * </pre>
 * @since 3.2
 */
public class MongoDbMetadataRepository implements ReplicaStore, EnvironmentAware, ServerListener {

    private final static String DB_URL = "pool.plugins.meta.mongo.url";
    private final static String DB_NAME = "pool.plugins.meta.mongo.db";
    private final static String DB_COLLECTION = "pool.plugins.meta.mongo.collection";

    protected static final Logger LOGGER
            = LoggerFactory.getLogger(MongoDbMetadataRepository.class);

    /**
     * pool's name which initializes the connection
     */
    private final String pool;

    /**
     * URL to use when connection to MongoDB
     */
    private String url;

    /**
     * name of database to use
     */
    private String dbName;

    /**
     * name of db collection to use
     */
    private String collectionName;

    /**
     * MongoDB client.
     */
    private MongoClient mongo;

    /**
     * MongoDB collection handle.
     */
    private MongoCollection<Document> collection;

    /**
     * Flag, which indicates client connection status.
     */
    private volatile boolean isOk;

    /**
     * {@link FileStore} used to store data files.
     */
    private final FileStore fileStore;

    public MongoDbMetadataRepository(FileStore fileStore, Path directory, String poolName) {
        this(fileStore, directory, poolName, false);
    }

    public MongoDbMetadataRepository(FileStore fileStore, Path ignored, String poolName, boolean isReadOnly) {
        this.fileStore = fileStore;
        this.pool = poolName;
        this.isOk = false;
    }

    @Override
    public void init() throws CacheException {

        MongoClientOptions.Builder optionBuilder = new MongoClientOptions.Builder()
                .addServerListener(this)
                .description(pool)
                .applicationName("dCache-" + Version.of(MongoDbMetadataRepository.class).getVersion());

        mongo = new MongoClient(new MongoClientURI(url, optionBuilder));
        collection = mongo.getDatabase(dbName).getCollection(collectionName);
    }

    @Override
    public Set<PnfsId> index(IndexOption... options) throws CacheException {
        try {
            List<IndexOption> indexOptions = asList(options);

            if (indexOptions.contains(IndexOption.META_ONLY)) {
                return StreamSupport.stream(collection.find().spliterator(), false)
                        .map(d -> d.getString("pnfsid"))
                        .filter(PnfsId::isValid)
                        .map(PnfsId::new)
                        .collect(toSet());
            }

            Stopwatch watch = Stopwatch.createStarted();
            Set<PnfsId> files = fileStore.index();
            LOGGER.info("Indexed {} entries in {} in {}.", files.size(), fileStore, watch);

            if (indexOptions.contains(IndexOption.ALLOW_REPAIR)) {
                watch.reset().start();
                List<String> metaFilesToBeDeleted;
                try (Stream<Document> list = StreamSupport.stream(collection.find().spliterator(), false)) {
                    metaFilesToBeDeleted = list
                            .map(d -> d.getString("pnfsid"))
                            .filter(PnfsId::isValid)
                            .filter(id -> !files.contains(new PnfsId(id)))
                            .collect(toList());
                }
                LOGGER.info("Found {} orphaned meta data entries in {}.", metaFilesToBeDeleted.size(), watch);

                metaFilesToBeDeleted.forEach((id) -> {
                    deleteIfExists(id);
                });
            }
            return files;
        } catch (IOException e) {
            throw new DiskErrorCacheException("Meta data lookup failed and a pool restart is required: " + e.getMessage(), e);
        }
    }

    @Override
    public ReplicaRecord get(PnfsId id) throws CacheException {
        if (!fileStore.contains(id)) {
            return null;
        }
        try {
            return new CacheRepositoryEntryImpl(pool, id, collection, fileStore);
        } catch (IOException e) {
            throw new DiskErrorCacheException(
                    "Failed to read meta data for " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public ReplicaRecord create(PnfsId id, Set<? extends OpenOption> flags) throws DuplicateEntryException, CacheException {
        if (fileStore.contains(id)) {
            throw new DuplicateEntryException(id);
        }

        try {
            if (flags.contains(StandardOpenOption.CREATE)) {
                fileStore.create(id);
            }
            return new CacheRepositoryEntryImpl(pool, id, collection, fileStore);
        } catch (IOException e) {
            throw new DiskErrorCacheException(
                    "Failed to create new entry " + id + ": " + e.getMessage(), e);

        }
    }

    @Override
    public void remove(PnfsId id) throws CacheException {
        try {
            fileStore.remove(id);
            deleteIfExists(id);

        } catch (IOException | MongoException e) {
            isOk = false;
            throw new DiskErrorCacheException("Failed to remove " + id + ": " + e.getMessage(), e);
        }
    }

    private void deleteIfExists(String id) {

        Document dbKey = new Document()
                .append("pool", pool)
                .append("pnfsid", id);

        long n = collection.deleteMany(dbKey).getDeletedCount();

        if (n > 1) {
            LOGGER.warn("Removed more than on entry ({}) for pnfsid: {}", n, id);
        }
    }

    private void deleteIfExists(PnfsId id) {
        deleteIfExists(id.toIdString());
    }

    @Override
    public boolean isOk() {
        return isOk;
    }

    @Override
    public void close() {
        mongo.close();
    }

    @Override
    public long getFreeSpace() {
        try {
            return fileStore.getFreeSpace();
        } catch (IOException e) {
            LOGGER.warn("Failed to query free space: {}", e.toString());
            return 0;
        }
    }

    @Override
    public long getTotalSpace() {
        try {
            return fileStore.getTotalSpace();
        } catch (IOException e) {
            LOGGER.warn("Failed to query total space: {}", e.toString());
            return 0;
        }
    }

    @Override
    public String toString() {
        return String.format("[data=%s;meta=%s]", fileStore, mongo.getConnectPoint());
    }

    @Override
    public void setEnvironment(Map<String, Object> environment) {
        url = (String)environment.get(DB_URL);
        dbName = (String)environment.get(DB_NAME);
        collectionName = (String)environment.get(DB_COLLECTION);
    }

    @Override
    public void serverOpening(ServerOpeningEvent event) {
        LOGGER.info("Connected to server {}", event.getServerId());
        isOk = true;
    }

    @Override
    public void serverClosed(ServerClosedEvent event) {
        LOGGER.info("Lost connection with server {}", event.getServerId());
        isOk = false;
    }

    @Override
    public void serverDescriptionChanged(ServerDescriptionChangedEvent event) {
        // NOP
    }
}
