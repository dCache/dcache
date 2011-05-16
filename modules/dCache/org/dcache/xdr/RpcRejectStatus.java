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
 * Reasons why a call message was rejected.
 */
public final class RpcRejectStatus {

    private RpcRejectStatus() {}
    /**
     * RPC version number != 2.
     */
    public static final int RPC_MISMATCH = 0;
    /**
     * Remote can't authenticate caller.
     */
    public static final int AUTH_ERROR = 1;

    public static String toString(int i) {
        switch(i) {
            case RPC_MISMATCH: return "RPC_MISMATCH";
            case AUTH_ERROR: return "AUTH_ERROR";
        }
        return "Unknown: " +i;
    }
}
