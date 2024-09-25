package org.dcache.chimera.spi;

import static org.dcache.util.SqlHelper.tryToClose;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsSqlDriver;
import org.dcache.chimera.H2FsSqlDriver;

public class H2DBDriverProvider implements DBDriverProvider {

    @Override
    public boolean isSupportDB(DataSource dataSource) throws SQLException {

        Connection dbConnection = null;
        try {
            dbConnection = dataSource.getConnection();
            String databaseProductName = dbConnection.getMetaData().getDatabaseProductName();

            return databaseProductName.equalsIgnoreCase("H2");
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public FsSqlDriver getDriver(DataSource dataSource, String consistency) throws SQLException, ChimeraFsException {
        return new H2FsSqlDriver(dataSource);
    }
}
