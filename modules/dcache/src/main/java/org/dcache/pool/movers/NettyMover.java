/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013-2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.movers;

import java.util.UUID;

import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellPath;

import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.ReplicaDescriptor;

/**
 * A mover that served by AbstractNettyTransferServices.
 */
public class NettyMover<P extends ProtocolInfo> extends MoverChannelMover<P, NettyMover<P>>
{
    private final UUID uuid;


    public NettyMover(ReplicaDescriptor handle,
                      PoolIoFileMessage message,
                      CellPath pathToDoor,
                      TransferService<NettyMover<P>> transferService,
                      UUID uuid,
                      ChecksumModule checksumModule)
    {
        super(handle, message, pathToDoor, transferService, MoverChannel.AllocatorMode.HARD, checksumModule);
        this.uuid = uuid;
    }

    public UUID getUuid()
    {
        return uuid;
    }
}
