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

import javax.security.auth.Subject;

public interface RpcAuth extends XdrAble {

    /**
     * Get the authentication flavor. The rfc 1831 defines the following flavors:
     * <pre>
     *   AUTH_NONE  = 0
     *   AUTH_SYS   = 1
     *   AUTH_SHORT = 2
     * </pre>
     *
     * @return auth flavor.
     */
    int type();

    /**
     * Get authentication verified corresponding to this credentials.
     * In case of AUTH_NONE or AUTH_SYS the verifier is empty. Some other
     * auth flavors may have non empty verifies ( RPCGSS_SEC contains the CRC
     * of RPC header ).
     *
     * @return verifier.
     */
    RpcAuthVerifier getVerifier();

    /**
     * Get {@link Subject} associated with credentials.
     * @return subject.
     */
    Subject getSubject();
}
