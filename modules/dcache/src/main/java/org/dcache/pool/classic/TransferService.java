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
package org.dcache.pool.classic;

import java.nio.channels.CompletionHandler;
import org.dcache.pool.movers.Mover;

/**
 * Transfer service interface.
 * <p>
 * Implementations of this interface have the ability to execute the transfer described by a mover.
 */
public interface TransferService<M extends Mover<?>> {

    Cancellable executeMover(M mover, CompletionHandler<Void, Void> completionHandler)
          throws Exception;

    void closeMover(M mover, CompletionHandler<Void, Void> completionHandler);
}
