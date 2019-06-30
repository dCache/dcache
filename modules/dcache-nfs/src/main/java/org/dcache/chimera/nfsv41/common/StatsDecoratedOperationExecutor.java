/*
 * Copyright (c) 2019 Deutsches Elektronen-Synchroton,
 * Member of the Helmholtz Association, (DESY), HAMBURG, GERMANY
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera.nfsv41.common;

import java.io.IOException;

import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.OperationExecutor;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.oncrpc4j.rpc.OncRpcException;


/**
 *
 * A decorator for {@link OperationExecutor} that collects operation execution
 * statistics.
 */
public class StatsDecoratedOperationExecutor implements OperationExecutor {

    /**
     * Request execution gauges.
     */
    private final RequestExecutionTimeGauges<String> gauges
            = new RequestExecutionTimeGauges<>(StatsDecoratedOperationExecutor.class.getName());


    private final OperationExecutor inner;
    public StatsDecoratedOperationExecutor(OperationExecutor inner) {
        this.inner = inner;
    }

    @Override
    public nfs_resop4 execute(CompoundContext context, nfs_argop4 args) throws IOException, OncRpcException {

        long t0 = System.nanoTime();
        try {
            return inner.execute(context, args);
        } finally {
            gauges.update(nfs_opnum4.toString(args.argop), System.nanoTime() - t0);
        }
    }

    public RequestExecutionTimeGauges<String> getStatistics() {
        return gauges;
    }
}
