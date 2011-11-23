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

package org.dcache.chimera.nfs.v4;

import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;

/**
 *
 * NFSv4 operation abstraction
 *
 */
public abstract class AbstractNFSv4Operation {

    protected final nfs_resop4 _result = new nfs_resop4();
    protected final nfs_argop4 _args;

    public AbstractNFSv4Operation(nfs_argop4 args, int opCode) {
        _result.resop = opCode;
        _args = args;
    }

    /**
     * Process current operation.
     * @return result of operation
     */
    public abstract nfs_resop4 process(CompoundContext context);
}
