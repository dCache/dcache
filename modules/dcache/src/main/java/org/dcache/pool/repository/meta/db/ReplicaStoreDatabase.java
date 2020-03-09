package org.dcache.pool.repository.meta.db;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * MetaDataRepositoryDatabase encapsulates the initialisation of
 * the BerkelyDB used for storing meta data.
 */
public class ReplicaStoreDatabase
{
    private static final Logger _log =
        LoggerFactory.getLogger("logger.org.dcache.repository");

    private final Environment env;

    private static final String CLASS_CATALOG = "java_class_catalog";
    private static final String STORAGE_INFO_STORE = "storage_info_store";
    private static final String STATE_STORE = "state_store";
    private static final String LAST_ACCESS_STORE = "last_access_store";
    private static final String CREATION_TIME_STORE = "creation_time_store";




    private final StoredClassCatalog javaCatalog;
    private final Database storageInfoDatabase;
    private final Database stateDatabase;
    private final Database lastAccessDatabase;
    private final Database creationTimeDatabase;



    private final TransactionRunner transactionRunner;

    private boolean _failed;
    private boolean _closed;

    public ReplicaStoreDatabase(Properties properties, File homeDirectory, boolean readonly)
        throws DatabaseException
    {
        EnvironmentConfig envConfig = new EnvironmentConfig(properties);
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setReadOnly(readonly);

        env = new Environment(homeDirectory, envConfig);

        envConfig.setExceptionListener(event -> {
            if (event.getException() instanceof EnvironmentFailureException && !env.isValid()) {
                setFailed();
                _log.error("Pool restart required due to Berkeley DB failure: {}", event.getException().getMessage());
            }
        });

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setReadOnly(readonly);

        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);

        javaCatalog = new StoredClassCatalog(catalogDb);

        storageInfoDatabase =
            env.openDatabase(null, STORAGE_INFO_STORE, dbConfig);
        stateDatabase = env.openDatabase(null, STATE_STORE, dbConfig);

        lastAccessDatabase = env.openDatabase(null, LAST_ACCESS_STORE, dbConfig);


        creationTimeDatabase = env.openDatabase(null, CREATION_TIME_STORE, dbConfig);


        transactionRunner = new TransactionRunner(env);
    }

    private synchronized void setFailed()
    {
        _failed = true;
    }

    public synchronized boolean isFailed()
    {
        return _failed;
    }

    public synchronized void close()
        throws DatabaseException
    {
        if (!_closed) {
            creationTimeDatabase.close();
            lastAccessDatabase.close();
            stateDatabase.close();
            storageInfoDatabase.close();
            javaCatalog.close();
            env.close();
            _closed = true;
        }
    }

    public final Environment getEnvironment()
    {
        return env;
    }

    public void run(TransactionWorker worker) throws Exception
    {
        transactionRunner.run(worker);
    }

    public final StoredClassCatalog getClassCatalog()
    {
        return javaCatalog;
    }

    public final Database getStorageInfoDatabase()
    {
        return storageInfoDatabase;
    }

    public final Database getStateDatabase()
    {
        return stateDatabase;
    }

    public final Database getLastAccessDatabase()
    {
        return lastAccessDatabase;
    }

    public final Database getCreationTimeDatabase()
    {
        return creationTimeDatabase;
    }


    public DiskOrderedCursor openKeyCursor()
    {
        DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
        config.setKeysOnly(true);
        return env.openDiskOrderedCursor(new Database[]{storageInfoDatabase, stateDatabase}, config);
    }
}
