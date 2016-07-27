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

package org.dcache.cells;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.ExecutionException;

import diskCacheV111.vehicles.Message;

/**
 * Adapts a Future of a Message to be a Reply.
 *
 * <p>Once the future completes, the reply is submitted.
 */
public class FutureReply<T extends Message> extends MessageReply<T> implements Runnable
{
    private final T request;

    private final ListenableFuture<? extends T> future;

    public FutureReply(T request, ListenableFuture<? extends T> future)
    {
        this.request = request;
        this.future = future;
        future.addListener(this, MoreExecutors.directExecutor());
    }

    @Override
    public void run()
    {
        try {
            T msg = future.get();
            if (msg != null) {
                reply(msg);
            }
        } catch (InterruptedException e) {
            fail(request, e);
        } catch (ExecutionException e) {
            fail(request, e.getCause());
        }
    }
}
