package org.dcache.chimera.spi;

import static org.dcache.util.SqlHelper.tryToClose;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsSqlDriver;
import org.dcache.chimera.HsqlDBFsSqlDriver;

public class HsqlDBDriverProvider implements DBDriverProvider {

    @Override
    public boolean isSupportDB(DataSource dataSource) throws SQLException {

        Connection dbConnection = null;
        try {
            dbConnection = dataSource.getConnection();
            String databaseProductName = dbConnection.getMetaData().getDatabaseProductName();

            return databaseProductName.equalsIgnoreCase("HSQL Database Engine");
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public FsSqlDriver getDriver(DataSource dataSource) throws SQLException, ChimeraFsException {
        return new HsqlDBFsSqlDriver(dataSource);
    }

}
