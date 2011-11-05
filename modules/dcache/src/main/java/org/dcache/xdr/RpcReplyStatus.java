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
 * A reply to a call message can take on two forms:
 * The message was either accepted or rejected.
 */
public final class RpcReplyStatus {

    private RpcReplyStatus() {}
    /**
     * The message was accepted.
     */
    public static final int MSG_ACCEPTED = 0;

    /**
     * The message was rejected.
     */
    public static final int MSG_DENIED = 1;

    public static String toString(int i) {
        switch(i) {
            case MSG_ACCEPTED: return "MSG_ACCEPTED";
            case MSG_DENIED: return "MSG_DENIED";
        }
        return "Unknown " + i;
    }
}
