package org.dcache.tests.repository;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.jolbox.bonecp.BoneCPDataSource;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import diskCacheV111.util.PnfsId;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.HFile;
import org.dcache.chimera.IOHimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.pool.repository.FileStore;

public class RepositoryHealerTestChimeraHelper implements FileStore {


    private static final String JDBC_URL = "jdbc:hsqldb:mem:";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";

    private final UUID uuid = UUID.randomUUID();
    private final JdbcFs _fs;
    private final FsInode _rootInode;


    public RepositoryHealerTestChimeraHelper() throws Exception {


        // FIXME: make it configurable
        Class.forName("org.hsqldb.jdbcDriver");

        BoneCPDataSource ds = new BoneCPDataSource();
        ds.setJdbcUrl(JDBC_URL + uuid);
        ds.setUsername(USERNAME);
        ds.setPassword(PASSWORD);
        ds.getConfig().setMaxConnectionsPerPartition(3); // seems to require >= 3
        ds.getConfig().setMinConnectionsPerPartition(1);
        ds.getConfig().setPartitionCount(1);

        String sql = Resources.toString(Resources.getResource("org/dcache/chimera/sql/create-hsqldb.sql"), Charsets.US_ASCII);
        try (Connection conn = ds.getConnection()) {
            try (Statement st = conn.createStatement()) {
                for (String statement : sql.replace("\n", "").split(";")) {
                    st.executeUpdate(statement);
                }
            }
        }

        _fs = new JdbcFs(ds, "HsqlDB");
        _rootInode = _fs.path2inode("/");
    }

    public void shutdown()
    {
        try {
            _fs.close();
            try (Connection conn = DriverManager.getConnection(JDBC_URL + uuid, USERNAME, PASSWORD)) {
                conn.createStatement().execute("SHUTDOWN;");
            }
        } catch (SQLException | IOException ignored) {
        }
    }

    public FsInode add(PnfsId pnfsid) throws ChimeraFsException {

        return _fs.createFile(_rootInode, pnfsid.toString() );

    }


    static void tryToClose(Statement o) {
        try {
            if (o != null) {
                o.close();
            }
        } catch (SQLException e) {

        }
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
}
