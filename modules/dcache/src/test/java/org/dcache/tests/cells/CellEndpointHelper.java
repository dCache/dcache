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
package org.dcache.tests.cells;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;

import org.dcache.util.Args;
import static com.google.common.base.Preconditions.checkState;

/**
 * Helper class to mock a CellEndpoint. To be used with CellStubHelper.
 */
public class CellEndpointHelper implements CellEndpoint
{
    private volatile CellStubHelper _test;
    private final String _cellName;

    public CellEndpointHelper(String cellName)
    {
        _cellName = cellName;
    }

    public void execute(CellStubHelper test) throws Exception
    {
        _test = test;
        try {
            test.run();
        } finally {
            _test = null;
        }
    }

    private CellStubHelper currentTest() {
        CellStubHelper test = _test;
        checkState(test != null);
        return test;
    }

    @Override
    public void sendMessage(CellMessage envelope)
            throws SerializationException, NoRouteToCellException
    {
        currentTest().messageArrived(envelope);
    }

    @Override
    public void sendMessage(CellMessage envelope, CellMessageAnswerable callback, Executor executor, long timeout)
            throws SerializationException
    {
        CellMessage answer = currentTest().messageArrived(envelope);
        Object obj = answer.getMessageObject();
        if (obj instanceof Exception) {
            callback.exceptionArrived(envelope, (Exception) obj);
        } else {
            callback.answerArrived(envelope, answer);
        }
    }

    @Override
    public void sendMessageWithRetryOnNoRouteToCell(CellMessage envelope, CellMessageAnswerable callback,
                                                    Executor executor, long timeout)
            throws SerializationException
    {
        sendMessage(envelope, callback, executor, timeout);
    }

    @Override
    public CellInfo getCellInfo()
    {
        CellInfo info = new CellInfo();
        info.setDomainName("test");
        info.setCellName(_cellName);
        return info;
    }

    @Override
    public Map<String, Object> getDomainContext()
    {
        return Collections.emptyMap();
    }

    @Override
    public Args getArgs()
    {
        return new Args("");
    }
}
