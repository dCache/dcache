package org.dcache.chimera.spi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsSqlDriver;
import org.dcache.chimera.PgSQL95FsSqlDriver;

import static org.dcache.util.SqlHelper.tryToClose;

public class PgSQLDrivertProvider implements DBDriverProvider {

    @Override
    public boolean isSupportDB(DataSource dataSource) throws SQLException {

        Connection dbConnection = null;
        try {
            dbConnection = dataSource.getConnection();
            String databaseProductName = dbConnection.getMetaData().getDatabaseProductName();

            if (databaseProductName.equalsIgnoreCase("PostgreSQL")) {
                /*
                 * CockroachDB presents itself as PostgreSQL. However,
                 * on-the-wire is only supported. As a result, we can't use any
                 * server-side optimizations. IOW, CockroachDB is not PostgreSQL.
                 */
                try (ResultSet rs = dbConnection.getMetaData().getSchemas()) {
                    while(rs.next()) {
                        String schema = rs.getString("TABLE_SCHEM");
                        if (schema.equalsIgnoreCase("crdb_internal")) {
                             return false;
                        }
                    }
                }
                return true;
            }
            return false;
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public FsSqlDriver getDriver(DataSource dataSource) throws SQLException, ChimeraFsException {

        Connection dbConnection = null;
        try {
            dbConnection = dataSource.getConnection();

            int maj = dbConnection.getMetaData().getDatabaseMajorVersion();
            int min = dbConnection.getMetaData().getDatabaseMinorVersion();

            if ((maj > 9) || (maj == 9 && min >= 5)) {
                return new PgSQL95FsSqlDriver(dataSource);
            } else {
                throw new IllegalArgumentException("Required PostgreSQL 9.5 or newer");
            }
        } finally {
            tryToClose(dbConnection);
        }
    }

}
