package org.dcache.tests.repository;

import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import diskCacheV111.util.PnfsId;
import java.net.URL;
import java.util.Properties;
import java.util.UUID;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.HFile;
import org.dcache.chimera.IOHimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.pool.repository.FileStore;

public class RepositoryHealerTestChimeraHelper implements FileStore {

    private final static URL DB_TEST_PROPERTIES
            = Resources.getResource("org/dcache/tests/repository/chimera-test.properties");

    private final Connection _conn;
    private final JdbcFs _fs;
    private final FsInode _rootInode;

    public RepositoryHealerTestChimeraHelper() throws Exception {
        Properties dbProperties = new Properties();
        dbProperties.load(Resources.newInputStreamSupplier(DB_TEST_PROPERTIES).getInput());

        DataSource ds = getDataSource(
                dbProperties.getProperty("chimera.db.url") + ":" + UUID.randomUUID(),
                dbProperties.getProperty("chimera.db.user"),
                dbProperties.getProperty("chimera.db.password"));

        _conn = ds.getConnection();

        _conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(_conn));
        Liquibase liquibase = new Liquibase("org/dcache/chimera/changelog/changelog-master.xml",
                new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        _fs = new JdbcFs(ds, dbProperties.getProperty("chimera.db.dialect"));
        _rootInode = _fs.path2inode("/");
    }

    public void shutdown()
    {
        try {
            _fs.close();
            _conn.createStatement().execute("SHUTDOWN;");
            //_conn.close();
        } catch (SQLException | IOException ignored) {
        }
    }

    public FsInode add(PnfsId pnfsid) throws ChimeraFsException {

        return _fs.createFile(_rootInode, pnfsid.toString() );
    }

    @Override
    public File get(PnfsId id) {
        return new HFile(_fs, id.toString() );
    }


    @Override
    public long getFreeSpace() {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long getTotalSpace() {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public boolean isOk() {
        return true;
    }


    @Override
    public List<PnfsId> list() {


        List<PnfsId> entries = new ArrayList<>();


        try {
            String[] list = _fs.listDir(_rootInode);

            for(String entry: list) {
                entries.add( new PnfsId(entry) );
            }

        } catch (IOHimeraFsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return entries;
    }

    public static HikariDataSource getDataSource(String url, String user, String pass) {
        HikariConfig config = new HikariConfig();
        config.setDataSource(new DriverManagerDataSource(url, user, pass));
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(3);
        return new HikariDataSource(config);
    }
}
