package org.dcache.chimera.nfsv41.door;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.zaxxer.hikari.HikariDataSource;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.NoLabelChimeraException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.Inode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

public class ChimeraVfsTest {

    private final static URL DB_TEST_PROPERTIES =
          Resources.getResource("org/dcache/chimera/chimera-test.properties");

    protected FileSystemProvider _fs;
    protected FsInode _rootInode;
    protected HikariDataSource _dataSource;

    private Connection _conn;
    private JdbcFs _fileFileSystemProvider;
    private ChimeraVfs _chimeraVfs;

    @Before
    public void setUp() throws Exception {

        Properties dbProperties = new Properties();
        try (InputStream input = Resources.asByteSource(DB_TEST_PROPERTIES).openStream()) {
            dbProperties.load(input);
        }

        _dataSource = FsFactory.getDataSource(
              dbProperties.getProperty("chimera.db.url"),
              dbProperties.getProperty("chimera.db.user"),
              dbProperties.getProperty("chimera.db.password"));

        try (Connection conn = _dataSource.getConnection()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            Database database = DatabaseFactory.getInstance()
                  .findCorrectDatabaseImplementation(new JdbcConnection(conn));
            Liquibase liquibase = new Liquibase("org/dcache/chimera/changelog/changelog-master.xml",
                  new ClassLoaderResourceAccessor(), database);

            liquibase.update("");
        }

        PlatformTransactionManager txManager = new DataSourceTransactionManager(_dataSource);
        _fs = new JdbcFs(_dataSource, txManager, "strong");
        _fileFileSystemProvider = (JdbcFs) _fs;
        _rootInode = _fs.path2inode("/");

        _chimeraVfs = new ChimeraVfs(_fileFileSystemProvider, null);


    }

    @After
    public void tearDown() throws Exception {
        Connection conn = _dataSource.getConnection();
        conn.createStatement().execute("SHUTDOWN;");
        _dataSource.close();
        _fs.close();
    }


    @Test
    public void shouldReturnLabeledFileList() throws Exception {

        FsInode _rootInode;
        _rootInode = _fs.path2inode("/");

        FsInode dir = _rootInode.mkdir("parent");

        FsInode inodeA = _fs.createFile(dir, "aFile");
        FsInode inodeB = _fs.createFile(dir, "bFile");
        FsInode inodeC = _fs.createFile(dir, "cFile");

        String labelnameCat = "cat";
        String labelnameDog = "dog";

        _fs.addLabel(inodeA, labelnameCat);
        _fs.addLabel(inodeB, labelnameCat);
        _fs.addLabel(inodeC, labelnameDog);

        Inode inode = _chimeraVfs.lookup(_chimeraVfs.getRootInode(), ".(collection)(cat)");

        byte[] verifier = {};

        DirectoryStream result = _chimeraVfs.list(inode, verifier, 0);
        Collection<String> dirLs = new HashSet<>();

        Iterator<DirectoryEntry> i = result.iterator();

        while (i.hasNext()) {

            DirectoryEntry entry = i.next();

            FsInode newInnode = _chimeraVfs.toFsInode(entry.getInode());
            dirLs.add(newInnode.getId());

        }
        assertTrue(dirLs.containsAll(Lists.newArrayList(inodeA.getId(), inodeB.getId())));

    }


    @Test(expected = NoEntException.class)
    public void shouldReturnPathDoesNotExist() throws Exception {

        FsInode _rootInode;
        _rootInode = _fs.path2inode("/");

        FsInode dir = _rootInode.mkdir("parent");

        FsInode inodeA = _fs.createFile(dir, "aFile");

        String labelnameCat = "cat";

        _fs.addLabel(inodeA, labelnameCat);

        Inode inode = _chimeraVfs.lookup(_chimeraVfs.getRootInode(), ".(collection)(newLabel)");

    }


}
