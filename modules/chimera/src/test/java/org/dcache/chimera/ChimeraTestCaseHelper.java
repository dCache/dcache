package org.dcache.chimera;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.dcache.commons.util.SqlHelper;
import org.junit.After;
import org.junit.Before;

public abstract class ChimeraTestCaseHelper {

    protected FileSystemProvider _fs;
    protected FsInode _rootInode;
    private Connection _conn;

    @Before
    public void setUp() throws Exception {

        Class.forName("org.hsqldb.jdbcDriver");

        _conn = DriverManager.getConnection("jdbc:hsqldb:mem:chimeramem", "sa", "");
        _conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        StringBuilder sql = new StringBuilder();

        BufferedReader dataStr =
            new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("org/dcache/chimera/sql/create-hsqldb.sql")));
        String inLine = null;

        while ((inLine = dataStr.readLine()) != null) {
            sql.append(inLine);
        }

        String[] statements = sql.toString().split(";");
        for (String statement : statements) {
            Statement st = _conn.createStatement();
            st.executeUpdate(statement);
            SqlHelper.tryToClose(st);
        }

        _fs = ChimeraFsHelper.getFileSystemProvider("jdbc:hsqldb:mem:chimeramem",
                "org.hsqldb.jdbcDriver",
                "sa", "", "HsqlDB");
        _rootInode = _fs.path2inode("/");

    }

    @After
    public void tearDown() throws Exception {
        _fs.close();
        _conn.createStatement().execute("SHUTDOWN;");
        _conn.close();
    }
}
