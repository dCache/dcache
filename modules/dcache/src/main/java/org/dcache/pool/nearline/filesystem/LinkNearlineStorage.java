/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.nearline.filesystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LinkNearlineStorage extends FileSystemNearlineStorage
{
    public LinkNearlineStorage(String type, String name)
    {
        super(type, name);
    }

    @Override
    protected void flush(Path path, Path externalPath) throws IOException
    {
        Files.createLink(externalPath, path);
    }

    @Override
    protected void stage(Path externalPath, Path path) throws IOException
    {
        Files.createLink(path, externalPath);
    }

    @Override
    protected void remove(Path externalPath) throws IOException
    {
        Files.deleteIfExists(externalPath);
    }
}
