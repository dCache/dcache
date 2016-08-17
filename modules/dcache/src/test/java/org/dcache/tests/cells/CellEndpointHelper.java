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

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.SerializationException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

/**
 * Helper class to mock a CellEndpoint. To be used with CellStubHelper.
 */
public class CellEndpointHelper implements CellEndpoint
{
    private volatile CellStubHelper _test;
    private CellAddressCore _address;

    public CellEndpointHelper(CellAddressCore address)
    {
        _address = address;
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
    public void sendMessage(CellMessage envelope, SendFlag... flags)
            throws SerializationException
    {
        if (!asList(flags).contains(SendFlag.PASS_THROUGH)) {
            envelope.addSourceAddress(_address);
        }
        currentTest().messageArrived(envelope);
    }

    @Override
    public void sendMessage(CellMessage envelope, CellMessageAnswerable callback, Executor executor,
                            long timeout, SendFlag... flags)
            throws SerializationException
    {
        if (!asList(flags).contains(SendFlag.PASS_THROUGH)) {
            envelope.addSourceAddress(_address);
        }
        CellMessage answer = currentTest().messageArrived(envelope);
        Object obj = answer.getMessageObject();
        if (obj instanceof Exception) {
            callback.exceptionArrived(envelope, (Exception) obj);
        } else {
            callback.answerArrived(envelope, answer);
        }
    }

    @Override
    public Map<String, Object> getDomainContext()
    {
        return Collections.emptyMap();
    }
}
