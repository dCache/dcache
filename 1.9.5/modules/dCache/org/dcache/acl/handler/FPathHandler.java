package org.dcache.acl.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.dcache.acl.ACLException;
import org.dcache.acl.config.Config;
import org.dcache.acl.util.sql.SQLHandler;

/**
 * Component takes absolute filename and translates it into a unique resource ID.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class FPathHandler extends THandler {

    private static final Logger logger = Logger.getLogger("logger.org.dcache.authorization." + FPathHandler.class.getName());

    private static final String FILE_SEPARATOR = System.getProperty("file.separator", "/");

    private static final String ROOT_ID = "000000000000000000000000000000000000";

    private static final String COL_NAME = "iname";

    private static final String COL_PARENT = "iparent";

    private static final String COL_ID = "ipnfsid";

    private static final String TABLE_DIRS = "t_dirs";

    private static final String SQLP_SELECT_ID = "SELECT " + COL_ID + " FROM " + TABLE_DIRS + " WHERE " + COL_PARENT + " = ? AND " + COL_NAME + " = ?";

    // private Config config;
    //
    // private DataSource ds_pooled;

     public FPathHandler() throws ACLException {
        super(new Config(System.getProperty("fpath.configuration", "fpath.properties")));
    }


//	/**
//	 * @param configFile
//	 *            FPath Handler Configuration file
//	 */
//	public FPathHandler(String config) throws ACLException {
//		super(new Config(config));
//	}

    //
    // /**
    // * @param config
    // * FPath Handler Configuration
    // */
    // public FPathHandler(Config config) throws ACLException {
    // initialize(config);
    // }

    /**
     * @param config
     *            FPath Handler Configuration
     */
    public FPathHandler(Config config) throws ACLException {
        super(config);
    }

    private String getID(String parentID, String name) throws ACLException {
        if ( logger.isDebugEnabled() )
            logger.debug("Getting ID: [parentID = " + parentID + ", name = " + name + "]");

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String id = null;
        try {
            conn = _ds_pooled.getConnection();
            pstmt = conn.prepareStatement(SQLP_SELECT_ID);
            pstmt.setString(1, parentID);
            pstmt.setString(2, name);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                if ( id != null )
                    throw new ACLException("Get ID", "Not a unique ID: [parentID = " + parentID + ", name = " + name + "]");
                id = rs.getString(1);
            }

            if ( logger.isDebugEnabled() )
                logger.debug("Getted ID: " + id);

            return id;

        } catch (SQLException e) {
            throw new ACLException("Get ID", "SQLException", e);

        } finally {
            SQLHandler.attemptClose(rs);
            SQLHandler.attemptClose(pstmt);
            SQLHandler.attemptClose(conn);
        }
    }

    public String getID(String path) throws IllegalArgumentException, ACLException {
        String[] split = path.split(FILE_SEPARATOR);
        if ( split == null )
            throw new IllegalArgumentException("path can't be splitted: " + path);

        int len = split.length;
        if ( len < 1 )
            throw new IllegalArgumentException("Count tags invalid in path: " + path);

        String id = null;
        String parentID = ROOT_ID;
        for (String name : split) {
            if ( name.length() != 0 ) {
                id = getID(parentID, name);
                parentID = id;
            }
        }
        return id;
    }

}