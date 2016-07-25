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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.net.URI;
import java.util.Collections;
import java.util.Set;

import diskCacheV111.util.PnfsId;
import org.dcache.pool.movers.IoMode;

/**
 * File store to be used with meta data utilities. Returns the same temporary file for
 * all PNFS IDs. Depending on constructor arguments, the file may or may not exist.
 */
class DummyFileStore implements FileStore
{
    public final Path file;

    public enum Mode { ALL_EXIST, NONE_EXIST }

    DummyFileStore(Mode mode) throws IOException
    {
        file = Files.createTempFile("dcache-yaml-tool", null);
        file.toFile().deleteOnExit();
        if (mode == Mode.NONE_EXIST) {
            Files.delete(file);
        }
    }

    @Override
    public BasicFileAttributeView getFileAttributeView(PnfsId id) throws IOException {
        return Files.getFileAttributeView(file, BasicFileAttributeView.class);
    }

    @Override
    public URI create(PnfsId id) throws IOException {
        // NOP (already created)
        return get(id);
    }

    @Override
    public RepositoryChannel openDataChannel(PnfsId id, IoMode mode) throws IOException {
        return new FileRepositoryChannel(file, mode.toOpenString());
    }

    @Override
    public URI get(PnfsId id)
    {
        return file.toUri();
    }

    @Override
    public boolean contains(PnfsId id) {
        return true;
    }

    @Override
    public void remove(PnfsId id) {
        // NOP
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
