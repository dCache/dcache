/*
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

package org.dcache.xdr;

import java.io.IOException;
import java.util.Map;

import com.sun.grizzly.Context;
import com.sun.grizzly.ProtocolFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcDispatcher implements ProtocolFilter {

    private final static Logger _log = LoggerFactory.getLogger(RpcDispatcher.class);

    /**
     * List of registered RPC services
     *
     */
    private final Map<OncRpcProgram, RpcDispatchable> _programs;

    /**
     * Create new RPC dispatcher for given program.
     *
     * @param programs {@link Map}
     *     with a mapping between program number and program
     *     handler.
     *
     * @throws NullPointerException if programs is null
     */
    public RpcDispatcher(  Map<OncRpcProgram, RpcDispatchable> programs  )
        throws NullPointerException{

        if( programs == null)
            throw new NullPointerException("Programs is NULL");

        _programs = programs;
    }

    @Override
    public boolean execute(Context context) throws IOException {

        RpcCall call = (RpcCall)context.getAttribute(RpcProtocolFilter.RPC_CALL);

        int prog = call.getProgram();
        int vers = call.getProgramVersion();
        int proc = call.getProcedure();

        _log.debug("processing request {}", call);

        RpcDispatchable program = _programs.get( new OncRpcProgram(prog, vers));
        if( program == null ) {
            call.failProgramUnavailable();
        }else{
            try {
                program.dispatchOncRpcCall(call);
            } catch (OncRpcException e) {
                _log.error("Failed to process RPC request: {}", e.getMessage());
            }
        }

        return true;
    }

    @Override
    public boolean postExecute(Context context) throws IOException {
        return true;
    }

}
