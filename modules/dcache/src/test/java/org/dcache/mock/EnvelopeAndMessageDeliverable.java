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
package org.dcache.mock;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import java.io.IOException;
import java.io.Serializable;

public abstract class EnvelopeAndMessageDeliverable<R extends CellMessageReceiver> implements Deliverable<R> {
  protected CellAddressCore sourceAddress;

  protected abstract CellMessage buildEnvelope();

  @Override
  public void deliverTo(R receiver) throws IOException, InterruptedException
  {
    CellMessage envelope = buildEnvelope();
    envelope.nextDestination();
    Serializable request = envelope.getMessageObject();
    doDeliverTo(receiver, envelope, request);
  }

  protected abstract void doDeliverTo(R receiver, CellMessage envelope, Serializable request)
      throws IOException, InterruptedException;
}
