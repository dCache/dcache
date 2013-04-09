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
 * A PostTransferService is invoked after a file was transferred through a
 * TransferService.
 *
 * @since 1.9.11
 */
public interface PostTransferService
{
    /**
     * Submits a mover for processing by this post transfer service.
     *
     * @param mover mover to submit for post processing
     * @param completionHandler Callback signalling the completion of post processing
     */
    void execute(Mover<?> mover, CompletionHandler<Void, Void> completionHandler);
}
