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
 * Each RPC service has a program number. Because most new protocols evolve,
 * a version field of the call message identifies which version of the protocol
 * the caller is using. Version numbers enable support of both old and new
 * protocols through the same server process.
 *
 */
public class OncRpcProgram {

    /**
     * RPC program number.
     */
    private final int _number;

    /**
     * RPC program version number;
     */
    private final int _version;

    /**
     * Get program number.
     * @return program number
     */
    public int getNumber() {
        return _number;
    }

    /**
     * Get program version.
     * @return version number.
     */
    public int getVersion() {
        return _version;
    }

    public OncRpcProgram(int number, int version) {
        this._number = number;
        this._version = version;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }

        if ( ! (obj instanceof OncRpcProgram) ) {
            return false;
        }

        final OncRpcProgram other = (OncRpcProgram) obj;

        return (this._number == other._number) &&
                (this._version == other._version);
    }

    @Override
    public int hashCode() {
        return _number;
    }

}
