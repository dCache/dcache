package org.dcache.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlHelper.class);

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
            if (o != null) {
                o.close();
            }
        } catch (SQLException e) {
            LOGGER.error("failed to close prepared statement: {}", e.getMessage());
        }
    }

    /**
     * database resource cleanup
     *
     * @param o
     */
    public static void tryToClose(Statement o) {
        try {
            if (o != null) {
                o.close();
            }
        } catch (SQLException e) {
            LOGGER.error("failed to close result statement: {}", e.getMessage());
        }
    }

    /**
     * database resource cleanup
     *
     * @param o
     */
    public static void tryToClose(ResultSet o) {
        try {
            if (o != null) {
                o.close();
            }
        } catch (SQLException e) {
            LOGGER.error("failed to close result set: {}", e.getMessage());
        }
    }

    /**
     * database resource cleanup
     *
     * @param o
     */
    public static void tryToClose(Connection o) {
        try {
            if (o != null) {
                o.close();
            }
        } catch (SQLException e) {
            LOGGER.error("failed to close connection: {}", e.getMessage());
        }
    }

    public static void tryToRollback(Connection o) {
        try {
            o.rollback();
        } catch (SQLException e) {
            LOGGER.error("failed to rollback transaction: {}", e.getMessage());
        }
    }
}
