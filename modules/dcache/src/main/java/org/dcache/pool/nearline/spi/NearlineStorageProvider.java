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
package org.dcache.pool.nearline.spi;

/**
 * Factory of NearlineStorage.
 *
 * This is the entry point for providers of nearline storage.
 */
public interface NearlineStorageProvider
{
    /**
     * Returns a name uniquely identifying this provider.
     *
     * The name is used by the dCache administrator to refer to the provider. This
     * is not the same as the HSM instance name: The instance names identifies a
     * particular nearline storage, while the provider is merely a driver to
     * access a nearline storage. As such, the same provider can be used to access
     * several instances, and the same instance could possibly be accessed by
     * different providers.
     *
     * @return a string identifying the provider.
     */
    String getName();

    /**
     * One line human readable description of this provider.
     *
     * @return a description of the provider.
     */
    String getDescription();

    /**
     * Creates a new nearline storage instance.
     *
     * The new instance is identified by a unique instance name.
     *
     * Nearline storages in dCache have a type. The type essentially identifies the meta data schema
     * of the nearline storage and must be supported by the dCache name space provider.
     *
     * When a file is written to the nearline storage, the location is represented by a URI. The scheme
     * component of the URI is the nearline storage type, while the authority component is the nearline
     * storage instance name.
     *
     * @param type a dCache nearline storage type.
     * @param name an instance name uniquely identifying the instance.
     * @return a new NearlineStorage instance.
     */
    NearlineStorage createNearlineStorage(String type, String name);
}
