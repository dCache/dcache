package org.dcache.tests.repository;

import com.google.common.io.Resources;
import com.zaxxer.hikari.HikariDataSource;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import diskCacheV111.util.PnfsId;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.HFile;
import org.dcache.chimera.JdbcFs;
import org.dcache.pool.repository.FileStore;

public class RepositoryHealerTestChimeraHelper implements FileStore {

    private final static URL DB_TEST_PROPERTIES
            = Resources.getResource("org/dcache/tests/repository/chimera-test.properties");

    private final JdbcFs _fs;
    private final FsInode _rootInode;
    private final HikariDataSource _dataSource;

    public RepositoryHealerTestChimeraHelper() throws Exception {
        Properties dbProperties = new Properties();
        try (InputStream input = Resources.asByteSource(DB_TEST_PROPERTIES).openStream()) {
            dbProperties.load(input);
        }

        _dataSource = FsFactory.getDataSource(
                dbProperties.getProperty("chimera.db.url") + ":" + UUID.randomUUID(),
                dbProperties.getProperty("chimera.db.user"),
                dbProperties.getProperty("chimera.db.password"));

        try (Connection conn = _dataSource.getConnection()) {

            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn));
            Liquibase liquibase = new Liquibase("org/dcache/chimera/changelog/changelog-master.xml",
                    new ClassLoaderResourceAccessor(), database);
            liquibase.update("");
        }
        PlatformTransactionManager txManager =  new DataSourceTransactionManager(_dataSource);
        _fs = new JdbcFs(_dataSource, txManager);
        _rootInode = _fs.path2inode("/");
    }

    public void shutdown()
    {
        try {
            _dataSource.getConnection().createStatement().execute("SHUTDOWN;");
            _dataSource.close();
        } catch (SQLException ignored) {
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
    public Set<PnfsId> index() {


        Set<PnfsId> entries = new HashSet<>();


        try {
            String[] list = _fs.listDir(_rootInode);

            for(String entry: list) {
                entries.add( new PnfsId(entry) );
            }

        } catch (ChimeraFsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return entries;
    }
}
