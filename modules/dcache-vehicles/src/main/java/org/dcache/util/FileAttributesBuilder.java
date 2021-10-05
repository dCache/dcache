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

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import java.util.Collections;
import java.util.Set;
import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;

/**
 * A fluent class to build a FileAttributes object.
 */
public class FileAttributesBuilder {

    private final FileAttributes _attributes = new FileAttributes();

    public static FileAttributesBuilder fileAttributes() {
        return new FileAttributesBuilder();
    }

    public FileAttributesBuilder withSize(long size) {
        _attributes.setSize(size);
        return this;
    }

    public FileAttributesBuilder withSize(long size, ByteUnit units) {
        return withSize(units.toBytes(size));
    }

    public FileAttributesBuilder withLabel(String name) {
        return withLabels(Collections.singleton(name));
    }

    public FileAttributesBuilder withLabels(Set<String> names) {
        _attributes.setLabels(names);
        return this;
    }

    public FileAttributesBuilder withType(FileType type) {
        _attributes.setFileType(type);
        return this;
    }

    public FileAttributesBuilder withId(PnfsId id) {
        _attributes.setPnfsId(id);
        return this;
    }

    public FileAttributesBuilder withStorageInfo(StorageInfoBuilder builder) {
        return withStorageInfo(builder.build());
    }

    public FileAttributesBuilder withStorageInfo(StorageInfo info) {
        _attributes.setStorageInfo(info);
        return this;
    }

    public FileAttributesBuilder withXattr(String name, String value) {
        _attributes.updateXattr(name, value);
        return this;
    }

    public FileAttributes build() {
        return _attributes;
    }
}
