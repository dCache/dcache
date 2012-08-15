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

/**
 * The server is not running a compatible version
 * of the RPC protocol (RPC_MISMATCH).
 */
public class RpcMismatchReply extends RpcException {

    private static final long serialVersionUID = 5491675158956991722L;
    private final MismatchInfo _mismatchInfo;

    public RpcMismatchReply(int min, int max) {
        this(new MismatchInfo(min, max));
    }

    public RpcMismatchReply(MismatchInfo mismatchInfo) {
        super( RpcRejectStatus.RPC_MISMATCH, mismatchInfo.toString(), mismatchInfo );
        _mismatchInfo = mismatchInfo;
    }
}
