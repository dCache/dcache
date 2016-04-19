/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 1998 - 2016 Deutsches Elektronen-Synchrotron
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

package dmg.cells.nucleus;

import javax.annotation.concurrent.Immutable;

import java.io.Serializable;

@Immutable
public class CellTunnelInfo implements Serializable
{
    private static final long serialVersionUID = 6337314695599159656L;

    private final CellDomainInfo _remote;
    private final CellDomainInfo _local;
    private final CellAddressCore _tunnel;

    public CellTunnelInfo(CellAddressCore tunnel, CellDomainInfo local, CellDomainInfo remote)
    {
        _remote = remote;
        _local = local;
        _tunnel = tunnel;
    }

    public CellDomainInfo getRemoteCellDomainInfo()
    {
        return _remote;
    }

    public CellDomainInfo getLocalCellDomainInfo()
    {
        return _local;
    }

    public CellAddressCore getTunnel()
    {
        return _tunnel;
    }

    public String toString()
    {
        return _tunnel + " L[" + (_local != null ? _local.toString() : "Unknown") +
               "];R[" + (_remote != null ? _remote.toString() : "Unknown") + "]";
    }
}
