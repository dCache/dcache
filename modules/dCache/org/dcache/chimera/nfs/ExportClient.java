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


public class ExportClient {


    public enum Root {
        TRUSTED, NOTTRUSTED
    }

    public enum IO {
        RW, RO
    }

    private final String _ip;
    private final Root _isTrusted;
    private final IO _rw;

    public ExportClient(String ip, Root isTrusted, IO rw) {

        _ip = ip;
        _isTrusted = isTrusted;
        _rw = rw;

    }

    public String ip() {
        return _ip;
    }

    public IO io() {
        return _rw;
    }

    public Root trusted() {
        return _isTrusted;
    }

}
