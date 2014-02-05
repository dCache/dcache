package org.dcache.chimera;

import com.google.common.io.Resources;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.After;
import org.junit.Before;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public abstract class ChimeraTestCaseHelper {

     private final static URL DB_TEST_PROPERTIES =
        Resources.getResource("org/dcache/chimera/chimera-test.properties");

    protected FileSystemProvider _fs;
    protected FsInode _rootInode;
    private Connection _conn;

    @Before
    public void setUp() throws Exception {

        Properties dbProperties = new Properties();
        dbProperties.load(Resources.newInputStreamSupplier(DB_TEST_PROPERTIES).getInput());

        _conn = DriverManager.getConnection(dbProperties.getProperty("chimera.db.url"),
                dbProperties.getProperty("chimera.db.user"), dbProperties.getProperty("chimera.db.password"));

        _conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(_conn));
        Liquibase liquibase = new Liquibase("org/dcache/chimera/changelog/changelog-master.xml",
                new ClassLoaderResourceAccessor(), database);
        // Uncomment the following line when testing with mysql database
      /*
         * Liquibase liquibase = new Liquibase(changeLogFile, new
         * ClassLoaderResourceAccessor(), new JdbcConnection(conn));
         */

        liquibase.update("");
        _fs = FsFactory.getFileSystemProvider(
                dbProperties.getProperty("chimera.db.url"),
                dbProperties.getProperty("chimera.db.user"),
                dbProperties.getProperty("chimera.db.password"),
                dbProperties.getProperty("chimera.db.dialect"));
        _rootInode = _fs.path2inode("/");
    }

    @After
    public void tearDown() throws Exception {
        _fs.close();
        _conn.createStatement().execute("SHUTDOWN;");
        _conn.close();
    }

}
