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
import java.util.function.IntFunction;

/**
 * Deliver multiple messages.  The specific messages should be built using an
 * integer index, with the first message having index 1.
 */
public interface DeliveryRepeater <R extends CellMessageReceiver> {
  void repeatDeliveryTo(IntFunction<Deliverable<R>> messageBuilder, R container);
}
