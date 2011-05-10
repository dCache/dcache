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
 * Given that a call message was accepted, the following is the
 * status of an attempt to call a remote procedure.
 */
public final class  RpcAccepsStatus {

    private RpcAccepsStatus(){}

    /**
     * RPC executed successfully
     */
    public static final int SUCCESS  = 0;
    /**
     * Remote hasn't exported program.
     */
    public static final int PROG_UNAVAIL = 1;
    /**
     * Remote can't support version #.
     */
    public static final int PROG_MISMATCH = 2;
    /**
     * Program can't support procedure.
     */
    public static final int PROC_UNAVAIL = 3;
    /**
     * Procedure can't decode params.
     */
    public static final int GARBAGE_ARGS = 4;
    /**
     * Undefined system error
     */
    public static final int SYSTEM = 5;

    public static String toString(int status) {
        switch(status) {
            case SUCCESS: return "SUCCESS";
            case PROG_UNAVAIL: return "PROG_UNAVIAL";
            case PROG_MISMATCH: return "PROG_MISMATCH";
            case PROC_UNAVAIL: return "PROC_UNAVAIL";
            case GARBAGE_ARGS: return "GARBAGE_ARGS";
            case SYSTEM: return "SYSTEM";
        }
        return "UNKNOWN";
    }
}
