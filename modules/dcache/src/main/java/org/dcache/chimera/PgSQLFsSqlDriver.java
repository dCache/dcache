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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.dcache.commons.util.SqlHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.chimera.posix.Stat;

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
     * @throws java.sql.SQLException
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
        List<String> elements = new ArrayList<String>();
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

        List<FsInode> inodes = new ArrayList<FsInode>();

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