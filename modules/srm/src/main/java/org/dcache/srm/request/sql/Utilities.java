package org.dcache.srm.request.sql;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * This class contains static utility methods for use by the classes
 * in org.dcache.srm.request.sql.
 */
class Utilities
{
    private Utilities()
    {
    }

    /**
     * Given a database identifier (such as a table name) returns
     * the identifier as it would be stored in the database.
     */
    public static String getIdentifierAsStored(DatabaseMetaData md,
                                               String identifier)
        throws SQLException
    {
        if (md.storesUpperCaseIdentifiers()) {
            return identifier.toUpperCase();
        } else if (md.storesLowerCaseIdentifiers()) {
            return identifier.toLowerCase();
        } else {
            return identifier;
        }
    }
}
