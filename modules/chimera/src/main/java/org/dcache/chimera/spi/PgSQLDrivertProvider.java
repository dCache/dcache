package org.dcache.chimera.spi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsSqlDriver;
import org.dcache.chimera.PgSQL95FsSqlDriver;
import org.dcache.chimera.PgSQLFsSqlDriver;

import static org.dcache.util.SqlHelper.tryToClose;

public class PgSQLDrivertProvider implements DBDriverProvider {


    // pattern to match versions like: 1.2.3 or 1.2rc1 or 1.2
    private final static Pattern VERSION_PATTERN = Pattern.compile("(?<maj>\\d+)\\.(?<min>\\d+)(?:(\\.(\\d+))|(\\w+))?");

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
            String databaseProductVersion = dbConnection.getMetaData().getDatabaseProductVersion();

            Matcher m = VERSION_PATTERN.matcher(databaseProductVersion);

            int maj = 0;
            int min = 0;

            try {
                if (m.matches()) {
                    maj = Integer.parseInt(m.group("maj"));
                    min = Integer.parseInt(m.group("min"));
                }
            } catch (NumberFormatException ignored) {
            }

            if ((maj > 9) || (maj == 9 && min >= 5)) {
                return new PgSQL95FsSqlDriver(dataSource);
            } else {
                return new PgSQLFsSqlDriver(dataSource);
            }
        } finally {
            tryToClose(dbConnection);
        }
    }

}
