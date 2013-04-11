/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

import diskCacheV111.vehicles.PoolIoFileMessage;

import dmg.cells.nucleus.CellPath;

import org.dcache.pool.classic.PostTransferService;
import org.dcache.pool.movers.MoverChannelMover;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.Checksum;
import org.dcache.vehicles.XrootdProtocolInfo;

/**
 * A mover for xrootd transfers.
 */
public class XrootdMover extends MoverChannelMover<XrootdProtocolInfo, XrootdMover>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(XrootdMover.class);

    public XrootdMover(ReplicaDescriptor handle,
                       PoolIoFileMessage message,
                       CellPath pathToDoor,
                       XrootdTransferService xrootdTransferService,
                       PostTransferService postTransferService)
    {
        super(handle, message, pathToDoor, xrootdTransferService, postTransferService);
    }

    @Override
    public Set<Checksum> getActualChecksums()
    {
        return Collections.emptySet();
    }

    @Override
    public Set<Checksum> getExpectedChecksums()
    {
        return Collections.emptySet();
    }
}
