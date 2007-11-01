package org.dcache.pool.repository.meta.db;

import java.io.File;
import java.io.FileNotFoundException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.bind.serial.StoredClassCatalog;

/**
 * MetaDataRepositoryDatabase encapsulates the initialisation of
 * the BerkelyDB used for storing meta data.
 */ 
public class MetaDataRepositoryDatabase
{
    private Environment env;

    private static final String CLASS_CATALOG = "java_class_catalog";
    private static final String STORAGE_INFO_STORE = "storage_info_store";
    private static final String STATE_STORE = "state_store";

    private StoredClassCatalog javaCatalog;
    private Database storageInfoDatabase;
    private Database stateDatabase;

    public MetaDataRepositoryDatabase(File homeDirectory)
        throws DatabaseException, FileNotFoundException
    {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        env = new Environment(homeDirectory, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);

        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);

        javaCatalog = new StoredClassCatalog(catalogDb);
        
        storageInfoDatabase = 
            env.openDatabase(null, STORAGE_INFO_STORE, dbConfig);
        stateDatabase = env.openDatabase(null, STATE_STORE, dbConfig);

        Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        close();
                    } catch (DatabaseException e) {
                        // Not much we can do about this now.
                    }
                }
            });
     }

    public void close()
        throws DatabaseException
    {
        stateDatabase.close();
        storageInfoDatabase.close();
        javaCatalog.close();
        env.close();
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