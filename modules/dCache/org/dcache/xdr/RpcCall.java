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
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcCall {

    private final static Logger _log = LoggerFactory.getLogger(RpcCall.class);

    /**
     * XID number generator
     */
    private final static AtomicInteger NEXT_XID = new AtomicInteger(0);

    private int _xid;

    /**
     * Supported RPC protocol version
     */
    private final static int RPCVERS = 2;

    /**
     * RPC program number
     */
    private int _prog;

    /**
     * RPC program version number
     */
    private int _version;

    /**
     * RPC program procedure number
     */
    private int _proc;

    /**
     *  RPC protocol version number
     */
    private int _rpcvers;

    /**
     * Authentication credential.
     */
    private RpcAuth _cred;

    /**
     * RPC call transport.
     */
    private final XdrTransport _transport;

    /**
     * Call body.
     */
    private final Xdr _xdr;

    public RpcCall(int prog, int ver, RpcAuth cred, XdrTransport transport) {
        this(prog, ver, cred, new Xdr(Xdr.MAX_XDR_SIZE), transport);
    }

    public RpcCall(int prog, int ver, RpcAuth cred, Xdr xdr, XdrTransport transport) {
        _prog = prog;
        _version = ver;
        _cred = cred;
        _transport = transport;
        _xdr = xdr;
        _proc = 0;
    }

    public RpcCall(int xid, Xdr xdr, XdrTransport transport) {
        _xid = xid;
        _xdr = xdr;
        _transport = transport;
    }

    public RpcCall(int xid, int prog, int ver, int proc, RpcAuth cred, Xdr xdr, XdrTransport transport) {
        _xid = xid;
        _prog = prog;
        _version = ver;
        _proc = proc;
        _cred = cred;
        _xdr = xdr;
        _transport = transport;
        _rpcvers = RPCVERS;
    }

    public void accept() throws IOException, OncRpcException {
         _rpcvers = _xdr.xdrDecodeInt();
         if (_rpcvers != RPCVERS) {
            throw new RpcMismatchReply(_rpcvers, 2);
         }

        _prog = _xdr.xdrDecodeInt();
        _version = _xdr.xdrDecodeInt();
        _proc = _xdr.xdrDecodeInt();
        _cred = RpcCredential.decode(_xdr);
     }

    /**
     * Get RPC call program number.
     *
     * @return version number
     */
    public int getProgram() {
        return _prog;
    }

    /**
     * @return the RPC call program version
     */
    public int getProgramVersion() {
        return _version;
    }

    /**
     * @return the RPC call program procedure
     */
    public int getProcedure() {
        return _proc;
    }

    public RpcAuth getCredential() {
        return _cred;
    }

    /**
     * Get RPC {@XdrTransport} used by this call.
     * @return transport
     */
    public XdrTransport getTransport() {
        return _transport;
    }

    public int getXid() {
        return _xid;
    }

    public Xdr getXdr() {
        return _xdr;
    }

    @Override
    public String toString() {
        return String.format("RPCv%d call: program=%d, version=%d, procedure=%d",
                _rpcvers, _prog, _version, _proc);
    }

    /**
     * Reject the request with given status. The call can be rejected for two
     * reasons: either the server is not running a compatible version of the
     * RPC protocol (RPC_MISMATCH), or the server rejects the identity of the
     * caller (AUTH_ERROR).
     *
     * @see RpcRejectStatus
     * @param status
     * @param reason
     */
    public void reject(int status, XdrAble reason) {
        XdrEncodingStream xdr = _xdr;
        try {
            RpcMessage replyMessage = new RpcMessage(_xid, RpcMessageType.REPLY);
            xdr.beginEncoding();
            replyMessage.xdrEncode(_xdr);
            xdr.xdrEncodeInt(RpcReplyStatus.MSG_DENIED);
            xdr.xdrEncodeInt(status);
            reason.xdrEncode(_xdr);
            xdr.endEncoding();

            ByteBuffer message = xdr.body();
            _transport.send(message);

        } catch (OncRpcException e) {
            _log.warn("Xdr exception: {}", e.getMessage());
        } catch (IOException e) {
            _log.error("Failed send reply: {}", e.getMessage());
        }
    }
    /**
     * Send accepted reply to the client.
     *
     * @param reply
     */
    public void reply(XdrAble reply) {
        acceptedReply(RpcAccepsStatus.SUCCESS, reply);
    }

    public void acceptedReply(int state, XdrAble reply) {

        XdrEncodingStream xdr = _xdr;
        try {
            RpcMessage replyMessage = new RpcMessage(_xid, RpcMessageType.REPLY);
            xdr.beginEncoding();
            replyMessage.xdrEncode(_xdr);
            xdr.xdrEncodeInt(RpcReplyStatus.MSG_ACCEPTED);
            _cred.getVerifier().xdrEncode(xdr);
            xdr.xdrEncodeInt(state);
            reply.xdrEncode(xdr);
            xdr.endEncoding();

            ByteBuffer message = xdr.body();
            _transport.send(message);
            _xdr.close();

        } catch (OncRpcException e) {
            _log.warn("Xdr exception: {}", e.getMessage());
        } catch (IOException e) {
            _log.error("Failed send reply: {}", e.getMessage());
        }
    }

    /**
     * Retrieves the parameters sent within an ONC/RPC call message.
     *
     * @param args the call argument do decode
     * @throws OncRpcException
     */
    public void retrieveCall(XdrAble args) throws OncRpcException, IOException {
        args.xdrDecode(_xdr);
        _xdr.endDecoding();
    }

    /**
     * Reply to client with error program version mismatch.
     * Accepted message sent.
     *
     * @param min minimal supported version
     * @param max maximal supported version
     */
    public void failProgramMismatch(int min, int max) {
        acceptedReply(RpcAccepsStatus.PROG_MISMATCH, new MismatchInfo(min, max));
    }

    /**
     * Reply to client with error program unavailable.
     * Accepted message sent.
     */
    public void failProgramUnavailable() {
        acceptedReply(RpcAccepsStatus.PROG_UNAVAIL, XdrVoid.XDR_VOID);
    }

    /**
     * Reply to client with error procedure unavailable.
     */
    public void failProcedureUnavailable() {
        acceptedReply(RpcAccepsStatus.PROC_UNAVAIL, XdrVoid.XDR_VOID);
    }

    /**
     * Send call to remove RPC server.
     *
     * @param procedure the number of the procedure.
     * @param args the argument of the procedure.
     * @param result the result of the procedure
     * @throws OncRpcException
     * @throws IOException
     */
    public void call(int procedure, XdrAble args, XdrAble result)
            throws OncRpcException, IOException {

        this.call(procedure, args, result, Integer.MAX_VALUE);
    }

    /**
     * Send call to remove RPC server.
     *
     * @param procedure the number of the procedure.
     * @param args the argument of the procedure.
     * @param result the result of the procedure
     * @param timeout
     * @throws OncRpcException
     * @throws IOException
     */
    public void call(int procedure, XdrAble args, XdrAble result, int timeout)
            throws OncRpcException, IOException {

        int xid = NEXT_XID.incrementAndGet();

        _xdr.beginEncoding();
        RpcMessage rpcMessage = new RpcMessage(xid, RpcMessageType.CALL);
        rpcMessage.xdrEncode(_xdr);
        _xdr.xdrEncodeInt(RPCVERS);
        _xdr.xdrEncodeInt(_prog);
        _xdr.xdrEncodeInt(_version);
        _xdr.xdrEncodeInt(procedure);
        _cred.xdrEncode(_xdr);
        args.xdrEncode(_xdr);
        _xdr.endEncoding();

        _transport.getReplyQueue().registerKey(xid);
        ByteBuffer data = _xdr.body();
        _transport.send(data);

        RpcReply reply;
        try {
            reply = _transport.getReplyQueue().get(xid, timeout);
            if( reply == null ) {
                _log.info( "Did not get reply in time");
                throw new IOException("Did not get reply in time");
            }
        } catch (InterruptedException e) {
            _log.error( "call processing interrupted");
            throw new IOException(e.getMessage());
        }

        if(reply.isAccepted() && reply.getAcceptStatus() == RpcAccepsStatus.SUCCESS ) {
            reply.getReplyResult(result);
        } else {
            _log.info( "reply not succeeded {0}", reply);
            // FIXME: error handling here

            if( reply.isAccepted() ) {
                throw new OncRpcAcceptedException(reply.getAcceptStatus());
            }
            throw new OncRpcRejectedException(reply.getRejectStatus());
        }
    }
}
