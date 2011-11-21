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

package org.dcache.chimera.nfs;

public class ChimeraNFSException extends java.io.IOException {

    /**
     *
     */
    private static final long serialVersionUID = 4319461664218810541L;
    private final int nfsStatus;

    public ChimeraNFSException(int newStatus, String msg) {
        super(msg);
        nfsStatus = newStatus;
    }

    public int getStatus() {
        return nfsStatus;
    }

    @Override
    public String toString() {
        return super.toString() + " NFS status: " + nfsStatus;
    }
}
