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
package org.dcache.poolmanager;

/**
 * Request to pool manager to provide an updated PoolManagerHandler.
 *
 * <p>Will not generate a reply until a handler with a modified version is
 * available. If the TTL expires before an updated handler is available, no
 * reply may be generated.
 */
public class PoolMgrGetUpdatedHandler extends PoolMgrGetHandler
{
    private static final long serialVersionUID = -6335302547973525551L;

    private final SerializablePoolManagerHandler.Version version;

    public PoolMgrGetUpdatedHandler(SerializablePoolManagerHandler.Version version)
    {
        this.version = version;
    }

    public SerializablePoolManagerHandler.Version getVersion()
    {
        return version;
    }
}
