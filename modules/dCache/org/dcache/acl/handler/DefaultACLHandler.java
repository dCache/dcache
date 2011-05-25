package org.dcache.acl.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.ACLException;
import org.dcache.acl.config.AclConfig;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.RsType;
import org.dcache.acl.enums.Who;
import static org.dcache.commons.util.SqlHelper.*;

/**
 * Generic component for managing the ACLs. It provides an interface to the ACL database table and
 * methods to retrieve and manipulate ACLs.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class DefaultACLHandler extends THandler implements ACLHandler {

    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + DefaultACLHandler.class.getName());

    public static final String COLUMN_RS_ID = "rs_id";

    public static final String COLUMN_RS_TYPE = "rs_type";

    public static final String COLUMN_TYPE = "type";

    public static final String COLUMN_FLAGS = "flags";

    public static final String COLUMN_ACCESS_MSK = "access_msk";

    public static final String COLUMN_WHO = "who";

    public static final String COLUMN_WHO_ID = "who_id";

    public static final String COLUMN_ADDRESS_MSK = "address_msk";

    public static final String COLUMN_ACE_ORDER = "ace_order";

    private static String SQLP_SELECT_ACL;

    private static String SQLP_INSERT_ACL;

    private static String SQLP_DELETE_ACL;

    public DefaultACLHandler() throws ACLException {
        super(new AclConfig());
    }

    /**
     * @param configFile
     *            Configuration file
     */
    public DefaultACLHandler(String configFile) throws ACLException {
        super(new AclConfig(configFile));
    }

    /**
     * @param aclConfig
     *            ACL Configuration
     */
    public DefaultACLHandler(AclConfig aclConfig) throws ACLException {
        super(aclConfig);
    }

    protected void initPreparedStatements() {
        final String tableACL = getTableACL();
        SQLP_SELECT_ACL = "SELECT * FROM " + tableACL + " WHERE rs_id =  ? ORDER BY ace_order";
        SQLP_INSERT_ACL = "INSERT INTO " + tableACL + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        SQLP_DELETE_ACL = "DELETE FROM " + tableACL + " WHERE rs_id = ?";
    }

    public boolean isEnabled() {
        boolean enabled = ((AclConfig)_config).isAclEnabled();
        if ( logger.isDebugEnabled() )
            logger.debug("ACL Handler is " + (enabled ? "ENABLED" : "DISABLED."));
        return enabled;
    }

    /**
     * Returns the access control information of the resource specified by the resource ID
     * parameter.
     *
     * @param rsId
     *            resource ID
     * @return Returns ACL
     * @throws ACLException
     */
    // TODO: add rsId as parameter, in next implementation
    public ACL getACL(String rsId) throws ACLException {
        long startTime = 0;
        if ( logger.isDebugEnabled() )
            startTime = System.currentTimeMillis();

//		logger.debug("Getting ACL, rsID: " + rsId);

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = _ds_pooled.getConnection();
            pstmt = conn.prepareStatement(SQLP_SELECT_ACL);
            pstmt.setString(1, rsId);
            rs = pstmt.executeQuery();

            Integer rstype = null;
            List<ACE> aces = new ArrayList<ACE>();
            while (rs.next()) {
                if ( rstype == null )
                    rstype = new Integer(rs.getInt(COLUMN_RS_TYPE));
                else if ( rstype.intValue() != rs.getInt(COLUMN_RS_TYPE) )
                    throw new ACLException("Get ACL", "changeable RsType.");

                aces.add(new ACE(rs.getInt(COLUMN_TYPE) == 0 ? AceType.ACCESS_ALLOWED_ACE_TYPE : AceType.ACCESS_DENIED_ACE_TYPE,
                        rs.getInt(COLUMN_FLAGS),
                        rs.getInt(COLUMN_ACCESS_MSK),
                        Who.valueOf(rs.getInt(COLUMN_WHO)),
                        rs.getInt(COLUMN_WHO_ID),
                        rs.getString(COLUMN_ADDRESS_MSK)));
            }

            if ( aces.size() == 0 )
                return null;

            ACL acl = new ACL(rsId, rstype.intValue() == 0 ? RsType.DIR : RsType.FILE, aces);
            if ( logger.isDebugEnabled() ) {
                logger.debug("Getted ACL: " + acl.toNFSv4String());
                logger.debug("TIMING: Get ACL (" + aces.size() + " ACEs) in " + (System.currentTimeMillis() - startTime) + " msec");
            }
            return acl;

        } catch (SQLException e) {
            throw new ACLException("Get ACL", "SQLException", e);

        } finally {
            tryToClose(rs);
            tryToClose(pstmt);
            tryToClose(conn);
        }
    }

    /**
     * Sets the access control information of the resource specified by the resource ID parameter.
     *
     * @param acl
     *            ACL
     * @return Returns true if operation succeed, otherwise false
     * @throws ACLException
     */
    public boolean setACL(ACL acl) throws ACLException {
        String rsId = acl.getRsId();
        RsType rsType = acl.getRsType();
        long startTime = 0;
        if ( logger.isDebugEnabled() ) {
            logger.debug("Setting ACL: " + acl.toNFSv4String());
            startTime = System.currentTimeMillis();
        }

        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = _ds_pooled.getConnection();
            pstmt = conn.prepareStatement(SQLP_INSERT_ACL);
            if ( conn.getAutoCommit() )
                conn.setAutoCommit(false);

            List<ACE> aces = acl.getList();
            if ( aces.size() == 0 ) {
                logger.warn("SetACL: empty list of ACEs. Resource ID: " + rsId);
                return true;
            }

            int order = 0;
            for (ACE ace : aces) {
                // TODO: performance : try to use update in loop and finally commit
                pstmt.setString(1, rsId);
                pstmt.setInt(2, rsType.getValue());
                pstmt.setInt(3, ace.getType().getValue());
                pstmt.setInt(4, ace.getFlags());
                pstmt.setInt(5, ace.getAccessMsk());
                pstmt.setInt(6, ace.getWho().getValue());
                pstmt.setInt(7, ace.getWhoID());
                pstmt.setString(8, ace.getAddressMsk());
                pstmt.setInt(9, order);

                pstmt.addBatch();
                order++;
            }

            int[] numUpdates = pstmt.executeBatch();
            for (int i = 0; i < numUpdates.length; i++)
                if ( numUpdates[i] != 1 )
                    throw new ACLException("Set ACL", "Execute batch " + i + " failed. Number of rows updated = " + numUpdates[i]);

            conn.commit();
            if ( logger.isDebugEnabled() )
                logger.debug("TIMING: Set ACL (" + aces.size() + " ACEs) in " + (System.currentTimeMillis() - startTime) + " msec");

            return true;

        } catch (SQLException e) {
            logger.error("Rollback attempt on setACL operation. SQLException: " + e.getMessage());
            if ( conn != null )
                try {
                    conn.rollback();

                } catch (SQLException sqle) {
                    throw new ACLException("Rollback setACL", "SQLException", sqle);
                }

            throw new ACLException("Rollback attempted. Set ACL", "SQLException", e);


        } finally {
            tryToClose(pstmt);
            tryToClose(conn);
        }
    }

    /**
     * Removes the ACL specified by the resource ID paramenter.
     *
     * @param rsId
     *            resource ID
     * @return Returns number of removed ACEs
     * @throws ACLException
     */
    public int removeACL(String rsId) throws ACLException {
        int rowCnt = -1;
        long startTime = 0;
        if ( logger.isDebugEnabled() ) {
            logger.info("Removing ACL, rsID = " + rsId);
            startTime = System.currentTimeMillis();
        }

        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = _ds_pooled.getConnection();
            pstmt = conn.prepareStatement(SQLP_DELETE_ACL);
            if ( conn.getAutoCommit() )
                conn.setAutoCommit(false);

            pstmt.setString(1, rsId);
            rowCnt = pstmt.executeUpdate();

            if ( rowCnt > 0 )
                conn.commit();

            if ( logger.isDebugEnabled() )
                logger.debug("TIMING: Remove ACL (" + rowCnt + " ACEs) in " + (System.currentTimeMillis() - startTime) + " msec");

        } catch (SQLException e) {
            logger.error("Rollback attempt on removeACL operation. SQLException: " + e.getMessage());
            if ( conn != null )
                try {
                    conn.rollback();

                } catch (SQLException sqle) {
                    throw new ACLException("Rollback removeACL", "SQLException", sqle);
                }

            throw new ACLException("Rollback attempted. Remove ACL", "SQLException", e);

        } finally {
            tryToClose(pstmt);
            tryToClose(conn);
        }
        return rowCnt;
    }

    public AclConfig getConfig() {
        return (AclConfig)_config;
    }

    public void setConfig(AclConfig config) {
        _config = config;
    }

    public String getTableACL() {
        return ((AclConfig)_config).getACLTable();
    }

//	protected void attemptRollback(String action, Connection conn) throws ACLException {
//		if ( conn != null ) {
//			logger.error("Rollback attempt on " + action + " operation.");
//			try {
//				conn.rollback();
//
//			} catch (SQLException e) {
//				throw new ACLException("Rollback", e);
//			}
//		}
//	}

}
