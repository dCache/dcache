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

import com.google.common.util.concurrent.AbstractFuture;

import java.util.concurrent.TimeoutException;

/**
 * A class adapter that allows a future to be used in place of a CellMessageAnswerable.
 */
public class FutureCellMessageAnswerable extends AbstractFuture<CellMessage> implements CellMessageAnswerable
{
    @Override
    public void answerArrived(CellMessage request, CellMessage answer)
    {
        set(answer);
    }

    @Override
    public void exceptionArrived(CellMessage request, Exception exception)
    {
        setException(exception);
    }

    @Override
    public void answerTimedOut(CellMessage request)
    {
        setException(new TimeoutException());
    }
}
