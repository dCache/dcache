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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class FsFactory
{
    public static final String USAGE = "<jdbcUrl> <dbDialect> <dbUser> <dbPass>";
    public static final int ARGC = 4;

    public static FileSystemProvider createFileSystem(String[] args)
    {
        if (args.length < ARGC) {
            throw new IllegalArgumentException("Required argument missing: " + USAGE);
        }
        return getFileSystemProvider(args[0], args[2], args[3], args[1]);
    }

    public static FileSystemProvider getFileSystemProvider(
            String url, String user, String pass, String dialect)
    {
        HikariConfig config = new HikariConfig();
        config.setDataSource(new DriverManagerDataSource(url, user, pass));
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        return new JdbcFs(new HikariDataSource(config), dialect);
    }

    private static class DriverManagerDataSource implements DataSource
    {
        private final String url;
        private final String user;
        private final String pass;

        public DriverManagerDataSource(String url, String user, String pass)
        {
            this.url = url;
            this.user = user;
            this.pass = pass;
        }

        @Override
        public Connection getConnection() throws SQLException
        {
            return getConnection(user, pass);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException
        {
            return DriverManager.getConnection(url, username, password);
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException
        {
            throw new UnsupportedOperationException("getLogWriter");
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException
        {
            throw new UnsupportedOperationException("setLogWriter");
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException
        {
            throw new UnsupportedOperationException("setLoginTimeout");
        }

        @Override
        public int getLoginTimeout() throws SQLException
        {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException
        {
            throw new SQLFeatureNotSupportedException("getParentLogger");
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException
        {
            if (iface.isInstance(this)) {
                return (T) this;
            }
            throw new SQLException("DataSource of type [" + getClass().getName() +
                                           "] cannot be unwrapped as [" + iface.getName() + "]");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException
        {
            return iface.isInstance(this);
        }
    }
}
