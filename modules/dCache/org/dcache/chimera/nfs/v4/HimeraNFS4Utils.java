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

import org.dcache.chimera.nfs.v4.xdr.nfsace4;
import org.dcache.chimera.nfs.v4.xdr.utf8str_cs;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.utf8str_cis;
import org.dcache.chimera.nfs.v4.xdr.utf8str_mixed;
import org.dcache.chimera.nfs.v4.xdr.utf8string;

public class HimeraNFS4Utils {

    private HimeraNFS4Utils() { /* no instance allowed */ }

    public static String aceToString(nfsace4 ace) {

        String who = new String(ace.who.value.value);
        int type = ace.type.value.value;
        int flag = ace.flag.value.value;
        int mask = ace.access_mask.value.value;

    	return who + " " + aceType2String(type) + " " + Integer.toBinaryString(flag) + " " + Integer.toBinaryString(mask);

    }

    private static String aceType2String(int type) {

        switch (type) {

            case nfs4_prot.ACE4_ACCESS_ALLOWED_ACE_TYPE:
                return "ALLOW";
            case nfs4_prot.ACE4_ACCESS_DENIED_ACE_TYPE:
                return "DENY ";
            case nfs4_prot.ACE4_SYSTEM_ALARM_ACE_TYPE:
                return "ALARM";
            case nfs4_prot.ACE4_SYSTEM_AUDIT_ACE_TYPE:
                return "AUDIT";
            default:
                return ">BAD<";
        }

    }

    /**
     * Convert String to a case sensitive string of UTF-8 characters.
     * @param str
     * @return utf8str_cs representation of <i>str</i>
     */
    public static utf8str_cs string2utf8str_cs(String str) {
        return new utf8str_cs(new utf8string(str.getBytes()));
    }

    /**
     * Convert String to a case insensitive string of UTF-8 characters.
     * @param str
     * @return utf8str_cis representation of <i>str</i>
     */
    public static utf8str_cis string2utf8str_cis(String str) {
        return new utf8str_cis(new utf8string(str.getBytes()));
    }

    /**
     * Convert String to a case insensitive string of UTF-8 characters.
     * @param str
     * @return utf8str_mixed representation of <i>str</i>
     */
    public static utf8str_mixed string2utf8str_mixed(String str) {
        return new utf8str_mixed(new utf8string(str.getBytes()));
    }
}
