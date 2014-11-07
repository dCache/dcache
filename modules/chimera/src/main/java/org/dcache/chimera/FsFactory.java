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

import org.dcache.db.AlarmEnabledDataSource;

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
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(pass);
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        return new JdbcFs(new AlarmEnabledDataSource(url,
                                                     FsFactory.class.getSimpleName(),
                                                     new HikariDataSource(config)),
                                                     dialect);
    }
}
