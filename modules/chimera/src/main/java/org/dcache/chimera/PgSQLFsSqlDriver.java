/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.dcache.chimera.posix.Stat;
import org.dcache.commons.util.SqlHelper;

/**
 * PostgreSQL specific
 *
 *
 */
class PgSQLFsSqlDriver extends FsSqlDriver {

    /**
     * logger
     */
    private static final Logger _log = LoggerFactory.getLogger(PgSQLFsSqlDriver.class);

    /**
     *  this is a utility class which is issues SQL queries on database
     *
     */
    protected PgSQLFsSqlDriver() {
        _log.info("Running PostgreSQL specific Driver");
    }

    @Override
    FsInode mkdir(Connection dbConnection, FsInode parent, String name, int owner, int group, int mode,
                  Map<String, byte[]> tags) throws ChimeraFsException, SQLException
    {
        FsInode inode = mkdir(dbConnection, parent, name, owner, group, mode);
        /* There is a trigger that copies tags on mkdir, but we don't want those tags.
         */
        removeTag(dbConnection, inode);
        createTags(dbConnection, inode, owner, group, mode & 0666, tags);
        return inode;
    }

    private static final String sqlInode2Path = "SELECT inode2path(?)";
    private static final String sqlPath2Inode = "SELECT path2inode(?, ?)";
    private static final String sqlPath2Inodes = "SELECT ipnfsid,isize,inlink,itype,imode,iuid,igid,iatime,ictime,imtime from path2inodes(?, ?)";

    /**
     *
     * return the path associated with inode, starting from root of the tree.
     * in case of hard link, one of the possible paths is returned
     *
     * @param dbConnection
     * @param inode
     * @throws SQLException
     * @return
     */
    @Override
    String inode2path(Connection dbConnection, FsInode inode, FsInode startFrom, boolean inclusive) throws SQLException {

        String path = null;
        PreparedStatement ps = null;
        ResultSet result = null;

        try {

            ps = dbConnection.prepareStatement(sqlInode2Path);
            ps.setString(1, inode.toString());

            result = ps.executeQuery();

            if (result.next()) {
                path = result.getString(1);
            }

        } finally {
            SqlHelper.tryToClose(result);
            SqlHelper.tryToClose(ps);
        }

        return path;
    }

    /**
     * Returns a normalized path string for the given path. The
     * normalized string uses the slash character as a path separator,
     * it does not have a leading slash (i.e. it is relative), and it
     * has no empty path elements.
     */
    private String normalizePath(String path) {
        File file = new File(path);
        List<String> elements = new ArrayList<>();
        do {
            String fileName = file.getName();
            if (fileName.length() != 0) {
                /*
                 * skip multiple '/'
                 */
                elements.add(fileName);
            }

            file = file.getParentFile();
        } while (file != null);

        StringBuilder normalizedPath = new StringBuilder();
        if (!elements.isEmpty()) {
            normalizedPath.append(elements.get(elements.size() - 1));
            for (int i = elements.size() - 2; i >= 0; i--) {
                normalizedPath.append('/').append(elements.get(i));
            }
        }

        return normalizedPath.toString();
    }

    /**
     * get inode of given path starting <i>root</i> inode.
     * @param dbConnection
     * @param root staring point
     * @param path
     * @return inode or null if path does not exist.
     * @throws SQLException
     */
    @Override
    FsInode path2inode(Connection dbConnection, FsInode root, String path)
            throws SQLException, IOHimeraFsException {
        /* Ideally we would use the SQL array type for the second
         * parameter to inject the path elements, however there is no
         * easy way to do that with prepared statements. Hence we use
         * a slash delimited string instead. We cannot use
         * <code>path</code> as that uses the platform specific path
         * separator.
         */
        path = normalizePath(path);
        if (path.length() == 0) {
            return root;
        }

        PreparedStatement st = null;
        ResultSet result = null;
        try {
            st = dbConnection.prepareStatement(sqlPath2Inode);
            st.setString(1, root.toString());
            st.setString(2, path);
            result = st.executeQuery();
            if (result.next()) {
                String id = result.getString(1);
                if (id != null) {
                    return new FsInode(root.getFs(), id);
                }
            }
            return null;
        } finally {
            SqlHelper.tryToClose(result);
            SqlHelper.tryToClose(st);
        }
    }

    @Override
    List<FsInode>
        path2inodes(Connection dbConnection, FsInode root, String path)
        throws SQLException, IOHimeraFsException
    {
        /* Ideally we would use the SQL array type for the second
         * parameter to inject the path elements, however there is no
         * easy way to do that with prepared statements. Hence we use
         * a slash delimited string instead. We cannot use
         * <code>path</code> as that uses the platform specific path
         * separator.
         */
        path = normalizePath(path);

        if (path.length() == 0) {
            return Collections.singletonList(root);
        }

        List<FsInode> inodes = new ArrayList<>();

        PreparedStatement st = null;
        ResultSet result = null;
        try {
            st = dbConnection.prepareStatement(sqlPath2Inodes);
            st.setString(1, root.toString());
            st.setString(2, path);
            result = st.executeQuery();
            while (result.next()) {
                FsInode inode =
                    new FsInode(root.getFs(), result.getString("ipnfsid"));
                Stat stat = new Stat();
                stat.setSize(result.getLong("isize"));
                stat.setATime(result.getTimestamp("iatime").getTime());
                stat.setCTime(result.getTimestamp("ictime").getTime());
                stat.setMTime(result.getTimestamp("imtime").getTime());
                stat.setUid(result.getInt("iuid"));
                stat.setGid(result.getInt("igid"));
                stat.setMode(result.getInt("imode") | result.getInt("itype"));
                stat.setNlink(result.getInt("inlink"));
                stat.setIno((int) inode.id());
                stat.setDev(17);
                inode.setStatCache(stat);
                inodes.add(inode);
            }
        } finally {
            SqlHelper.tryToClose(result);
            SqlHelper.tryToClose(st);
        }

        return inodes;
    }

    private final static String sqlCreateEntryInParent = "insert into t_dirs (iparent, iname, ipnfsid) " +
            " (select ? as iparent, ? as iname, ? as ipnfsid " +
            " where not exists (select 1 from t_dirs where iparent=? and iname=?))";

    @Override
    void createEntryInParent(Connection dbConnection, FsInode parent, String name, FsInode inode) throws SQLException {
        PreparedStatement stInserIntoParent = null;
        try {

            stInserIntoParent = dbConnection.prepareStatement(sqlCreateEntryInParent);
            stInserIntoParent.setString(1, parent.toString());
            stInserIntoParent.setString(2, name);
            stInserIntoParent.setString(3, inode.toString());
            stInserIntoParent.setString(4, parent.toString());
            stInserIntoParent.setString(5, name);
            int n = stInserIntoParent.executeUpdate();
            if (n == 0) {
                /*
                 * no updates as such entry already exists.
                 * To be compatible with others, throw corresponding
                 * SQL exception.
                 */
                throw new SQLException("Entry already exists", DUPLICATE_KEY_ERROR);
            }

        } finally {
            SqlHelper.tryToClose(stInserIntoParent);
        }
    }

    private static final String ADD_INODE_LOCATION =
            "INSERT INTO t_locationinfo (SELECT ?,?,?,?,?,?,? WHERE NOT EXISTS " +
                    "(SELECT 1 FROM T_LOCATIONINFO WHERE ipnfsid=? AND itype=? AND ilocation=?))";

    @Override
    void addInodeLocation(Connection dbConnection, FsInode inode, int type, String location) throws SQLException
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        try (PreparedStatement stAddInodeLocation = dbConnection.prepareStatement(ADD_INODE_LOCATION)) {
            stAddInodeLocation.setString(1, inode.toString());
            stAddInodeLocation.setInt(2, type);
            stAddInodeLocation.setString(3, location);
            stAddInodeLocation.setInt(4, 10); // default priority
            stAddInodeLocation.setTimestamp(5, now);
            stAddInodeLocation.setTimestamp(6, now);
            stAddInodeLocation.setInt(7, 1); // online
            stAddInodeLocation.setString(8, inode.toString());
            stAddInodeLocation.setInt(9, type);
            stAddInodeLocation.setString(10, location);
            int n = stAddInodeLocation.executeUpdate();
            if (n == 0) {
                /*
                 * no updates as such entry already exists.
                 * To be compatible with others, throw corresponding
                 * SQL exception.
                 */
                throw new SQLException("Entry already exists", DUPLICATE_KEY_ERROR);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.dcache.chimera.FsSqlDriver#copyTags(java.sql.Connection, org.dcache.chimera.FsInode, org.dcache.chimera.FsInode)
     */
    @Override
    void copyTags(Connection dbConnection, FsInode orign, FsInode destination)
            throws SQLException {

        /*
         * There is a trigger which does it
         */
    }
}
