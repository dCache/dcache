/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.repository;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import diskCacheV111.util.PnfsId;

/**
 * File store to be used with meta data utilities. Returns the same temporary file for
 * all PNFS IDs. Depending on constructor arguments, the file may or may not exist.
 */
class DummyFileStore implements FileStore
{
    public final File file;

    public enum Mode { ALL_EXIST, NONE_EXIST }

    DummyFileStore(Mode mode) throws IOException
    {
        file = File.createTempFile("dcache-yaml-tool", null);
        file.deleteOnExit();
        if (mode == Mode.NONE_EXIST) {
            file.delete();
        }
    }

    @Override
    public File get(PnfsId id)
    {
        return file;
    }

    @Override
    public Set<PnfsId> index()
    {
        return Collections.emptySet();
    }

    @Override
    public long getFreeSpace()
    {
        return 0;
    }

    @Override
    public long getTotalSpace()
    {
        return 0;
    }

    @Override
    public boolean isOk()
    {
        return false;
    }
}
