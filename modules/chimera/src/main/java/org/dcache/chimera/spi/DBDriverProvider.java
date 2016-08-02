package org.dcache.chimera.spi;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FsSqlDriver;

/**
 * SPI interface to Driver provider. Provide must crate an instance of a
 * {@link FsSqlDriver} for a supported DB type.
 * @since 2.16
 */
public interface DBDriverProvider {

    /**
     * Check is provide support specific database type.
     *
     * @param dataSource source for database connection
     * @return true iff provider support database type.
     * @throws SQLException on db errors
     */
    boolean isSupportDB(DataSource dataSource) throws SQLException;

    /**
     * Get {@link FsSqlDriver} for the specific database.
     *
     * @param dataSource source for database connection
     * @return driver for specific database.
     * @throws SQLException on db errors
     */
    FsSqlDriver getDriver(DataSource dataSource) throws SQLException, ChimeraFsException;
}
