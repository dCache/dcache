package org.dcache.chimera.spi;

import com.google.common.base.Splitter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsSqlDriver;
import org.dcache.chimera.PgSQL95FsSqlDriver;
import org.dcache.chimera.PgSQLFsSqlDriver;

import static org.dcache.util.SqlHelper.tryToClose;

public class PgSQLDrivertProvider implements DBDriverProvider {

    @Override
    public boolean isSupportDB(DataSource dataSource) throws SQLException {

        Connection dbConnection = null;
        try {
            dbConnection = dataSource.getConnection();
            String databaseProductName = dbConnection.getMetaData().getDatabaseProductName();

            return databaseProductName.equalsIgnoreCase("PostgreSQL");
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
            List<String> versInfo = Splitter.on('.').splitToList(databaseProductVersion);
            int maj = 0;
            int min = 0;

            try {
                if (versInfo.size() >= 2) {
                    maj = Integer.parseInt(versInfo.get(0));
                    min = Integer.parseInt(versInfo.get(1));
                }
            } catch (NumberFormatException ignored) {
            }

            if (maj >= 9 && min >= 5) {
                return new PgSQL95FsSqlDriver(dataSource);
            } else {
                return new PgSQLFsSqlDriver(dataSource);
            }
        } finally {
            tryToClose(dbConnection);
        }
    }

}
