package org.dcache.commons.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;

public class SqlHelper {


    private SqlHelper() {
        // no instance allowed
    }

    /**
     * database resource cleanup
     *
     * @param o
     */
    public static void tryToClose(PreparedStatement o) {
        try {
            if (o != null)
                o.close();
        } catch (SQLException e) {
            // _logNamespace.error("tryToClose PreparedStatement", e);
        }
    }

    /**
     * database resource cleanup
     *
     * @param o
     */
    public static void tryToClose(Statement o) {
        try {
            if (o != null)
                o.close();
        } catch (SQLException e) {
            // _logNamespace.error("tryToClose PreparedStatement", e);
        }
    }

    /**
     * database resource cleanup
     *
     * @param o
     */
    public static void tryToClose(ResultSet o) {
        try {
            if (o != null)
                o.close();
        } catch (Exception e) {
            // _logNamespace.error("tryToClose ResultSet", e);
        }
    }

    /**
     * database resource cleanup
     *
     * @param o
     */
    public static void tryToClose(Connection o) {
        try {
            if (o != null)
                o.close();
        } catch (Exception e) {}
    }

}
