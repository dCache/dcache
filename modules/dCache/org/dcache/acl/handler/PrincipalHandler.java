package org.dcache.acl.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.acl.ACLException;
import org.dcache.acl.config.Config;
import org.dcache.acl.util.sql.SQLHandler;

/**
 * Component takes information on a principal on input and translates it into a virtual user ID
 * (virtUid) and a list of virtual group IDs (virtGid).
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class PrincipalHandler extends THandler {

    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + PrincipalHandler.class.getName());

    private final static String COL_ID = "id";

    private final static String COL_USER = "user_name";

    private final static String COL_GROUP = "group_name";

    private final static String COL_DN = "dn";

    private final static String TABLE_USERS = "t_users";

    private final static String TABLE_GROUPS = "t_groups";

    private final static int START_ID = 100;

    // Member variables
    // private Config config;
    //
    // private DataSource ds_pooled;

    public PrincipalHandler() throws ACLException {
        super(new Config(System.getProperty("ph.configuration", "ph.properties")));
    }

    // /**
    // * @param configFile
    // * Principal Handler Configuration file
    // */
    // public PrincipalHandler(String configFile) throws ACLException {
    // initialize(new Config(configFile));
    // }

    /**
     * @param config
     *            Principal Handler Configuration
     */
    public PrincipalHandler(Config config) throws ACLException {
        super(config);
    }

    private String getPrincipal(String table, String column, int id) throws SQLException {
        long startTime = 0;
        if ( logger.isDebugEnabled() ) {
            logger.debug("Getting Principal: [table = " + table + ", column = " + column + ", id = " + id + "]");
            startTime = System.currentTimeMillis();
        }

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String principal = null;
        try {
            conn = _ds_pooled.getConnection();
            pstmt = conn.prepareStatement("SELECT " + column + "  FROM " + table + " WHERE " + COL_ID + " = ?");
            pstmt.setInt(1, id);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                if ( principal != null )
                    throw new SQLException("Not a unique Principal: [table = " + table + ", column = " + column + ", id = " + id + "]");
                principal = rs.getString(1);
            }

            if ( logger.isDebugEnabled() ) {
                logger.debug("Principal from " + table + " for " + id + " is: " + principal);
                logger.debug("TIMING:Get Principal in " + (System.currentTimeMillis() - startTime) + " msec");
            }

            return principal;

        } finally {
            SQLHandler.attemptClose(rs);
            SQLHandler.attemptClose(pstmt);
            SQLHandler.attemptClose(conn);
        }
    }

    private Integer getNextID(String table) throws SQLException {
        long startTime = 0;
        if ( logger.isDebugEnabled() ) {
            logger.debug("Getting Max ID: [table = " + table + "]");
            startTime = System.currentTimeMillis();
        }

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Integer nextID = null;
        try {
            conn = _ds_pooled.getConnection();
            pstmt = conn.prepareStatement("SELECT max( " + COL_ID + ") FROM " + table);
            rs = pstmt.executeQuery();

            Integer max = null;
            while (rs.next()) {
                if ( max != null )
                    throw new SQLException("Not a unique max result: [table = " + table + "]");
                max = rs.getInt(1);
            }

            nextID = (max == null) ? START_ID : max + 1;

            if ( logger.isDebugEnabled() ) {
                logger.debug("Next ID in " + table + " is: " + max);
                logger.debug("TIMING: Get Max in " + (System.currentTimeMillis() - startTime) + " msec");
            }
            return nextID;

        } finally {
            SQLHandler.attemptClose(rs);
            SQLHandler.attemptClose(pstmt);
            SQLHandler.attemptClose(conn);
        }
    }

    private Integer getID(String table, String column, String value) throws SQLException {
        long startTime = 0;
        if ( logger.isDebugEnabled() ) {
            logger.debug("Getting ID: [table = " + table + ", column = " + column + ", value = " + value + "]");
            startTime = System.currentTimeMillis();
        }

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Integer id = null;
        try {
            conn = _ds_pooled.getConnection();
            pstmt = conn.prepareStatement("SELECT " + COL_ID + " FROM " + table + " WHERE " + column + " = ?");
            pstmt.setString(1, value);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                if ( id != null )
                    throw new SQLException("Not a unique ID: [table = " + table + ", column = " + column + ", value = " + value + "]");
                id = rs.getInt(1);
            }

            if ( logger.isDebugEnabled() ) {
                logger.debug("ID from " + table + " for " + value + " is: " + id);
                logger.debug("TIMING:Get ID in " + (System.currentTimeMillis() - startTime) + " msec");
            }
            return id;

        } finally {
            SQLHandler.attemptClose(rs);
            SQLHandler.attemptClose(pstmt);
            SQLHandler.attemptClose(conn);
        }
    }

    private Integer setID(String table, Integer id, String value) throws SQLException {
        long startTime = 0;
        if ( logger.isDebugEnabled() ) {
            logger.debug("Setting ID: [table = " + table + ", id = " + id + ", value = " + value + "]");
            startTime = System.currentTimeMillis();
        }

        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = _ds_pooled.getConnection();
            pstmt = conn.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)");
            pstmt.setInt(1, id);
            pstmt.setString(2, value);

            if ( conn.getAutoCommit() )
                conn.setAutoCommit(false);

            if ( pstmt.executeUpdate() != 1 )
                throw new SQLException("Not a unique update: [table = " + table + ", id = " + id + ", value = " + value + "]");

            conn.commit();

            if ( logger.isDebugEnabled() )
                logger.debug("TIMING: Set ID in " + (System.currentTimeMillis() - startTime) + " msec");
            return id;

        } catch (SQLException e) {
            logger.error("Rollback attempt on setID operation. SQLException: " + e.getMessage());
            if ( conn != null ) conn.rollback();
            throw new SQLException("Rollback attempted. Set ID failed: ", e.getMessage());

        } finally {
            SQLHandler.attemptClose(pstmt);
            SQLHandler.attemptClose(conn);
        }
    }

    public Integer getUserID(String user) throws IllegalArgumentException, ACLException {
        try {
            if ( user == null || user.length() == 0 )
                throw new IllegalArgumentException("user is " + (user == null ? "NULL" : "Empty"));

            Integer ID = getID(TABLE_USERS, COL_USER, user);
            if ( ID == null )
                ID = setID(TABLE_USERS, getNextID(TABLE_USERS), user);

            return ID;

        } catch (SQLException e) {
            throw new ACLException("Get user ID", "SQLException", e);
        }
    }

    public Integer getDNID(String dn) throws IllegalArgumentException, ACLException {
        try {
            if ( dn == null || dn.length() == 0 )
                throw new IllegalArgumentException("dn is " + (dn == null ? "NULL" : "Empty"));

            Integer ID = getID(TABLE_USERS, COL_DN, dn);
            if ( ID == null )
                ID = setID(TABLE_USERS, getNextID(TABLE_USERS), dn);

            return ID;

        } catch (SQLException e) {
            throw new ACLException("Get DN ID", "SQLException", e);
        }
    }

    public Integer getGroupID(String group) throws IllegalArgumentException, ACLException {
        try {
            if ( group == null || group.length() == 0 )
                throw new IllegalArgumentException("group is " + (group == null ? "NULL" : "Empty"));

            Integer ID = getID(TABLE_GROUPS, COL_GROUP, group);
            if ( ID == null )
                ID = setID(TABLE_GROUPS, getNextID(TABLE_USERS), group);

            return ID;

        } catch (SQLException e) {
            throw new ACLException("Get group ID", "SQLException", e);
        }
    }

    public String getUser(int id) throws IllegalArgumentException, ACLException {
        try {
            if ( id < 0 )
                throw new IllegalArgumentException("id = " + id);

            return getPrincipal(TABLE_USERS, COL_USER, id);

        } catch (SQLException e) {
            throw new ACLException("Get user", "SQLException", e);
        }
    }

    public String getDN(int id) throws IllegalArgumentException, ACLException {
        try {
            if ( id < 0 )
                throw new IllegalArgumentException("id = " + id);

            return getPrincipal(TABLE_USERS, COL_DN, id);

        } catch (SQLException e) {
            throw new ACLException("Get DN", "SQLException", e);
        }
    }

    public String getGroup(int id) throws IllegalArgumentException, ACLException {
        try {
            if ( id < 0 )
                throw new IllegalArgumentException("id = " + id);

            return getPrincipal(TABLE_GROUPS, COL_GROUP, id);

        } catch (SQLException e) {
            throw new ACLException("Get group", "SQLException", e);
        }
    }

}