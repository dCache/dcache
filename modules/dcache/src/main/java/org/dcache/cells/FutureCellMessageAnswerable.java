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

import com.google.common.util.concurrent.AbstractFuture;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;

import org.dcache.util.CacheExceptionFactory;

/**
 * A ListenableFuture that can be used as CellMessageAnswerable callback.
 */
public class FutureCellMessageAnswerable<T> extends AbstractFuture<T> implements CellMessageAnswerable
{
    protected final Class<? extends T> _type;

    public FutureCellMessageAnswerable(Class<? extends T> type)
    {
        _type = type;
    }

    @Override
    public void answerArrived(CellMessage request, CellMessage answer)
    {
        Object o = answer.getMessageObject();
        if (_type.isInstance(o)) {
            set(_type.cast(o));
        } else if (o instanceof Exception) {
            exceptionArrived(request, (Exception) o);
        } else {
            setException(new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                            "Unexpected reply: " + o));
        }
    }

    @Override
    public void answerTimedOut(CellMessage request)
    {
        setException(new TimeoutCacheException("Request to " + request.getDestinationPath() + " timed out."));
    }

    @Override
    public void exceptionArrived(CellMessage request, Exception exception)
    {
        if (exception.getClass() == CacheException.class) {
            CacheException e = (CacheException) exception;
            exception = CacheExceptionFactory.exceptionOf(e.getRc(), e.getMessage(), e);
        }
        setException(exception);
    }
}
