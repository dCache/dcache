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

import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import java.net.URI;

/**
 * A fluent class to build a StorageInfo.
 */
public class StorageInfoBuilder {

    private final GenericStorageInfo info = new GenericStorageInfo();

    public StorageInfoBuilder withLocation(String uri) {
        info.addLocation(URI.create(uri));
        return this;
    }

    public StorageInfoBuilder withHsm(String hsm) {
        info.setHsm(hsm);
        return this;
    }

    public StorageInfoBuilder withKey(String key, String value) {
        info.setKey(key, value);
        return this;
    }

    public StorageInfoBuilder withIsSetAddLocation() {
        info.isSetAddLocation(true);
        return this;
    }

    public StorageInfoBuilder withIsSetAddLocation(boolean value) {
        info.isSetAddLocation(value);
        return this;
    }

    public StorageInfo build() {
        return info;
    }

    public static StorageInfoBuilder aStorageInfo() {
        return new StorageInfoBuilder();
    }
}