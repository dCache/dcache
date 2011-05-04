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

import java.io.IOException;
import java.util.logging.Logger;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;

public class RpcAuthTypeNone implements RpcAuth, XdrAble {

    private final int _type =  RpcAuthType.NONE;
    private byte[] body;
    private RpcAuthVerifier _verifier = new RpcAuthVerifier(RpcAuthType.NONE, new byte[0]);
    private final Subject _subject = Subjects.NOBODY;

    private final static Logger _log = Logger.getLogger(RpcAuthTypeNone.class.getName());

    public RpcAuthTypeNone() {
        this(new byte[0]);
    }

    public RpcAuthTypeNone(byte[] body) {
        this.body = body;
    }

    @Override
    public Subject getSubject() {
        return _subject;
    }

    @Override
    public int type() {
        return _type;
    }

    @Override
    public RpcAuthVerifier getVerifier() {
        return _verifier;
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        body = xdr.xdrDecodeDynamicOpaque();
        _verifier = new RpcAuthVerifier(xdr);
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
       xdr.xdrEncodeInt(_type);
       xdr.xdrEncodeDynamicOpaque(body);
       _verifier.xdrEncode(xdr);
    }

}
