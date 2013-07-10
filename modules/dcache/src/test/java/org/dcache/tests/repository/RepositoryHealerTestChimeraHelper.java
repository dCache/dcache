package org.dcache.tests.repository;

import com.jolbox.bonecp.BoneCPDataSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import diskCacheV111.util.PnfsId;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.HFile;
import org.dcache.chimera.IOHimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.commons.util.SqlHelper;
import org.dcache.pool.repository.FileStore;

public class RepositoryHealerTestChimeraHelper implements FileStore {


    private Connection _conn;
    private JdbcFs _fs;
    private FsInode _rootInode;


    public RepositoryHealerTestChimeraHelper() throws Exception {


        // FIXME: make it configurable
        Class.forName("org.hsqldb.jdbcDriver");

        BoneCPDataSource ds = new BoneCPDataSource();
        ds.setJdbcUrl("jdbc:hsqldb:mem:chimeramem");
        ds.setUsername("sa");
        ds.setPassword("");
        ds.getConfig().setMaxConnectionsPerPartition(3); // seems to require >= 3
        ds.getConfig().setMinConnectionsPerPartition(1);
        ds.getConfig().setPartitionCount(1);

        _conn = ds.getConnection();

        StringBuilder sql = new StringBuilder();

        BufferedReader dataStr =
            new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("org/dcache/chimera/sql/create-hsqldb.sql")));
        String inLine;

        while ((inLine = dataStr.readLine()) != null) {
            sql.append(inLine);
        }

        String[] statements = sql.toString().split(";");
        for (String statement : statements) {
            Statement st = _conn.createStatement();
            st.executeUpdate(statement);
            SqlHelper.tryToClose(st);
        }

        _fs = new JdbcFs(ds, "HsqlDB");
        _rootInode = _fs.path2inode("/");
    }

    public void shutdown() {
        try {
            _conn.createStatement().execute("SHUTDOWN;");
            _conn.close();
        }catch (SQLException e) {
            // ignore
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
