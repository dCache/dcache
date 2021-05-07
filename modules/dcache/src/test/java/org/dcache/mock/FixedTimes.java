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

import dmg.cells.nucleus.CellMessageReceiver;
import java.io.IOException;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Deliver a message a fixed number times.  An iterator index (1-indexed) is
 * passed to the message builder function to allow it to build distinct
 * messages over the customisation.
 */
public class FixedTimes<R extends CellMessageReceiver> implements DeliveryRepeater<R>{
  private final int count;

  public FixedTimes(int count)
  {
    checkArgument(count > 0);
    this.count = count;
  }

  @Override
  public void repeatDeliveryTo(IntFunction<Deliverable<R>> messageBuilder, R container)
  {
    for (int i = 1; i <= count; i++) {
      try {
        var message = messageBuilder.apply(i);
        message.deliverTo(container);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
