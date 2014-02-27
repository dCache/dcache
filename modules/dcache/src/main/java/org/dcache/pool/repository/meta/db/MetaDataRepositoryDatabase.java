package org.dcache.pool.repository.meta.db;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.config.EnvironmentParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * MetaDataRepositoryDatabase encapsulates the initialisation of
 * the BerkelyDB used for storing meta data.
 */
public class MetaDataRepositoryDatabase
{
    private static Logger _log =
        LoggerFactory.getLogger("logger.org.dcache.repository");

    private final Environment env;

    private static final String CLASS_CATALOG = "java_class_catalog";
    private static final String STORAGE_INFO_STORE = "storage_info_store";
    private static final String STATE_STORE = "state_store";

    private final StoredClassCatalog javaCatalog;
    private final Database storageInfoDatabase;
    private final Database stateDatabase;
    private boolean _failed;
    private boolean _closed;

    public MetaDataRepositoryDatabase(File homeDirectory, boolean readonly)
        throws DatabaseException
    {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setReadOnly(readonly);
        envConfig.setConfigParam(EnvironmentConfig.MAX_MEMORY_PERCENT, "20");
        envConfig.setConfigParam(EnvironmentConfig.STATS_COLLECT, "false");
        envConfig.setExceptionListener(new ExceptionListener() {
                @Override
                public void exceptionThrown(ExceptionEvent event) {
                    if (event.getException() instanceof RunRecoveryException) {
                        setFailed();
                        _log.error("Pool restart required due to Berkeley DB failure: "
                                   + event.getException().getMessage());
                    }
                }
            });

        env = new Environment(homeDirectory, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setReadOnly(readonly);

        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);

        javaCatalog = new StoredClassCatalog(catalogDb);

        storageInfoDatabase =
            env.openDatabase(null, STORAGE_INFO_STORE, dbConfig);
        stateDatabase = env.openDatabase(null, STATE_STORE, dbConfig);
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
}
