/*
 * dCache - http://www.dcache.org/
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
package org.dcache.util;

import java.net.URI;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;

/**
 * A fluent class to build a FileAttribute.
 */
public class FileAttributesBuilder
{
    private final FileAttributes _attributes = new FileAttributes();

    public static class StorageInfoBuilder
    {
        private final GenericStorageInfo info = new GenericStorageInfo();

        public StorageInfoBuilder withLocation(String uri)
        {
            info.addLocation(URI.create(uri));
            return this;
        }

        public StorageInfo build()
        {
            return info;
        }
    }

    public static StorageInfoBuilder aStorageInfo()
    {
        return new StorageInfoBuilder();
    }

    public static FileAttributesBuilder attributes()
    {
        return new FileAttributesBuilder();
    }

    public FileAttributesBuilder size(long size)
    {
        _attributes.setSize(size);
        return this;
    }

    public FileAttributesBuilder size(long size, ByteUnit units)
    {
        return size(units.toBytes(size));
    }

    public FileAttributesBuilder type(FileType type)
    {
        _attributes.setFileType(type);
        return this;
    }

    public FileAttributesBuilder id(PnfsId id)
    {
        _attributes.setPnfsId(id);
        return this;
    }

    public FileAttributesBuilder storageInfo(StorageInfoBuilder builder)
    {
        return storageInfo(builder.build());
    }

    public FileAttributesBuilder storageInfo(StorageInfo info)
    {
        _attributes.setStorageInfo(info);
        return this;
    }

    public FileAttributes build()
    {
        return _attributes;
    }
}
