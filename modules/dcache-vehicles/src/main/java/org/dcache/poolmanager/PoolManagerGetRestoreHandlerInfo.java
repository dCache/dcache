/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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

import java.util.List;

import diskCacheV111.vehicles.PoolManagerMessage;
import diskCacheV111.vehicles.RestoreHandlerInfo;

/**
 * Request information about current request container tasks.
 */
public class PoolManagerGetRestoreHandlerInfo extends PoolManagerMessage
{
    private static final long serialVersionUID = 765552672615264580L;

    private List<RestoreHandlerInfo> result;

    public PoolManagerGetRestoreHandlerInfo()
    {
    }

    public PoolManagerGetRestoreHandlerInfo(List<RestoreHandlerInfo> result)
    {
        this.result = result;
    }

    public void setResult(List<RestoreHandlerInfo> result)
    {
        this.result = result;
    }

    public List<RestoreHandlerInfo> getResult()
    {
        return result;
    }
}
