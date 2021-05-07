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

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import java.io.Serializable;
import org.mockito.BDDMockito;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;

/**
 * Send a reply from a pool to the cell.  This is done by the
 * same thread sending the request.
 */
public abstract class ResponseMessageDeliverable<R extends CellMessageReceiver>
    extends EnvelopeAndMessageDeliverable<R> {
  protected final CellMessage outbound;
  protected Class<? extends Message> responseType;
  protected Serializable error;
  protected int code;

  protected ResponseMessageDeliverable(CellMessage outbound)
  {
    this.outbound = requireNonNull(outbound);
    configureOutboundAddress();
    // Simulate delivery of message to destination.
    outbound.addSourceAddress(sourceAddress);
    outbound.nextDestination();

    Serializable request = outbound.getMessageObject();
    checkArgument(request instanceof Message);
    responseType = (Class<? extends Message>) request.getClass();
  }

  public abstract ResponseMessageDeliverable aResponseTo(CellMessage message);

  public ResponseMessageDeliverable ofType(Class<? extends Message> type)
  {
    responseType = requireNonNull(type);
    return this;
  }

  public ResponseMessageDeliverable withError(Serializable error)
  {
    this.error = error;
    return this;
  }

  public ResponseMessageDeliverable withRc(int code)
  {
    this.code = code;
    return this;
  }

  protected abstract void configureOutboundAddress();

  @Override
  protected CellMessage buildEnvelope()
  {
    Message response = mock(responseType);
    if (error != null) {
      BDDMockito.given(response.getErrorObject()).willReturn(error);
    }
    BDDMockito.given(response.getReturnCode()).willReturn(code);

    outbound.revertDirection();
    outbound.setMessageObject(response);

    return outbound;
  }
}
