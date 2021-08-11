/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.chimera.namespace;

import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;

import static org.mockito.Mockito.mock;

/**
 * A builder class for creating FsInode objects.  The built objects are real,
 * but the FileSystemProvider is mocked.
 */
public class FsInodeBuilder
{
    private final FileSystemProvider fsProvider = mock(FileSystemProvider.class);
    private long ino = 0;

    public static FsInodeBuilder aFile()
    {
        return new FsInodeBuilder();
    }

    public FsInodeBuilder withInode(long id)
    {
        ino = id;
        return this;
    }

    public FsInode build()
    {
        return new FsInode(fsProvider, ino);
    }
}