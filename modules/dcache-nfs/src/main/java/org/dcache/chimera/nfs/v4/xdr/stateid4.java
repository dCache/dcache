/*
 * Copyright (c) 2009 - 2014 Deutsches Elektronen-Synchroton,
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
package org.dcache.chimera.nfs.v4.xdr;
import java.util.Arrays;

import java.io.Serializable;
import org.dcache.utils.Bytes;

public class stateid4 implements  Serializable {

    static final long serialVersionUID = -6677150504723505919L;

    public uint32_t seqid;
    public byte [] other;

    public stateid4(byte[] other, int seq) {
        this.other = other;
        seqid = new uint32_t(seq);
    }

    public stateid4(org.dcache.nfs.v4.xdr.stateid4 stateid) {
        this(stateid.other, stateid.seqid.value);
    }

    @Override
    public boolean equals(Object obj) {

        if( obj == this) return true;
        if( !(obj instanceof stateid4) ) return false;

        final stateid4 other_id = (stateid4) obj;

        return Arrays.equals(this.other, other_id.other);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(other);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("[");
        sb.append(Bytes.toHexString(other));
        sb.append(", seq: ").append(seqid.value).append("]");
        return sb.toString();
    }

}
// End of stateid4.java
