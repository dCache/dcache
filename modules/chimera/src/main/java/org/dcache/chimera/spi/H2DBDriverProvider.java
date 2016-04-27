package org.dcache.chimera.spi;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsSqlDriver;
import org.dcache.chimera.H2FsSqlDriver;
import static org.dcache.commons.util.SqlHelper.tryToClose;
import static org.dcache.commons.util.SqlHelper.tryToClose;

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
    public FsSqlDriver getDriver(DataSource dataSource) throws SQLException, ChimeraFsException {
        return new H2FsSqlDriver(dataSource);
    }
}
