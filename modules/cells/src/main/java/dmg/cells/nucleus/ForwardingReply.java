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
package dmg.cells.nucleus;

import java.io.Serializable;

/**
 * A Reply implementation that forwards the message to another cell instead
 * of replying back to the source.
 */
public class ForwardingReply implements Reply
{
    private final Serializable message;

    private CellPath address;

    public ForwardingReply(Serializable message)
    {
        this.message = message;
    }

    /**
     * Inject CellPath as the next destination of the message. If not set,
     * the message is forwarded to the next cell on the destination path.
     */
    public ForwardingReply via(CellPath address)
    {
        this.address = address;
        return this;
    }

    @Override
    public void deliver(CellEndpoint endpoint, CellMessage envelope)
    {
        if (address != null) {
            envelope.getDestinationPath().insert(address);
        }
        if (!envelope.nextDestination()) {
            throw new RuntimeException("Envelope cannot be forwarded as it has no next address: " + envelope);
        }
        envelope.setMessageObject(message);
        endpoint.sendMessage(envelope);
    }
}
