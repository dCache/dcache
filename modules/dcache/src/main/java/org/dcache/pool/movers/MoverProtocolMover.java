/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 - 2023 Deutsches Elektronen-Synchrotron
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

import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.CellPath;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.ReplicaDescriptor;

/**
 * A Mover implementation based on the MoverProtocol interface.
 */
public class MoverProtocolMover extends AbstractMover<ProtocolInfo, MoverProtocolMover> {

    /**
     * mover implementation suitable for this transfer
     */
    protected final MoverProtocol _moverProtocol;

    public MoverProtocolMover(ReplicaDescriptor handle, PoolIoFileMessage message,
          CellPath pathToDoor,
          TransferService<MoverProtocolMover> transferService,
          MoverProtocol moverProtocol) {
        super(handle, message, pathToDoor, transferService);
        _moverProtocol = moverProtocol;
    }

    @Override
    public long getTransferTime() {
        return _moverProtocol.getTransferTime();
    }

    @Override
    public long getBytesTransferred() {
        return _moverProtocol.getBytesTransferred();
    }

    @Override
    public long getLastTransferred() {
        return _moverProtocol.getLastTransferred();
    }

    public MoverProtocol getMover() {
        return _moverProtocol;
    }

    @Override
    protected String getStatus() {
        return _moverProtocol.toString();
    }

    @Override
    public List<InetSocketAddress> remoteConnections() {
        return _moverProtocol.remoteConnections();
    }

    @Override
    public Long getBytesExpected() {
        return _moverProtocol.getBytesExpected();
    }

    @Override
    public Optional<InetSocketAddress> getLocalEndpoint() {
        return _moverProtocol.getLocalEndpoint();
    }
}
