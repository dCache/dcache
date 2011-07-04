package org.dcache.xdr.gss;

import com.sun.grizzly.Context;
import com.sun.grizzly.ProtocolFilter;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.dcache.utils.Bytes;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.RpcAuthError;
import org.dcache.xdr.RpcAuthStat;
import org.dcache.xdr.RpcAuthType;
import org.dcache.xdr.RpcAuthVerifier;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.RpcException;
import org.dcache.xdr.RpcProtocolFilter;
import org.dcache.xdr.RpcRejectStatus;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProtocolFilter} that handles RPCSEC_GSS requests.
 * Filter is responsible to establish and destroy GSS context.
 * For requests with established contexts RPC requests repacked into
 * GSS aware {@link RpsGssCall}.
 *
 * @since 0.0.4
 */
public class GssProtocolFilter implements ProtocolFilter {

    private final static Logger _log = LoggerFactory.getLogger(GssProtocolFilter.class);
    /**
     * Return value from either accept or init stating that
     * the context creation phase is complete for this peer.
     * @see #init
     * @see #accept
     */
    public static final int COMPLETE = 0;
    /**
     * Return value from either accept or init stating that
     * another token is required from the peer to continue context
     * creation. This may be returned several times indicating
     * multiple token exchanges.
     * @see #init
     * @see #accept
     */
    public static final int CONTINUE_NEEDED = 1;

    private final GssSessionManager _gssSessionManager;

    public GssProtocolFilter(GssSessionManager gssSessionManager) {
        _gssSessionManager = gssSessionManager;
    }

    @Override
    public boolean execute(Context context) throws IOException {

        RpcCall call = (RpcCall) context.getAttribute(RpcProtocolFilter.RPC_CALL);

        if (call.getCredential().type() != RpcAuthType.RPCGSS_SEC) {
            return true;
        }

        boolean hasContext = false;
        try {
            RpcAuthGss authGss = (RpcAuthGss) call.getCredential();
            GSSContext gssContext = null;
            int _sequence = authGss.getSequence();
            switch (authGss.getProc()) {
                case GssProc.RPCSEC_GSS_INIT:
                    UUID uuid = UUID.randomUUID();
                    byte[] handle = new byte[16];
                    Bytes.putLong(handle, 0, uuid.getLeastSignificantBits());
                    Bytes.putLong(handle, 8, uuid.getMostSignificantBits());
                    gssContext = _gssSessionManager.createContext(handle);
                    authGss.setHandle(handle);
                    // fall through
                case GssProc.RPCSEC_GSS_CONTINUE_INIT:
                    if(gssContext == null)
                        gssContext =  _gssSessionManager.getContext(authGss.getHandle());
                    GSSINITargs gssArgs = new GSSINITargs();
                    GSSINITres res = new GSSINITres();
                    call.retrieveCall(gssArgs);
                    byte[] inToken = gssArgs.getToken();
                    byte[] outToken = gssContext.acceptSecContext(inToken, 0, inToken.length);
                    res.setHandle(authGss.getHandle());
                    res.setGssMajor(gssContext.isEstablished() ? COMPLETE : CONTINUE_NEEDED);
                    res.setGssMinor(0);
                    res.setToken(outToken);
                    if (gssContext.isEstablished()) {
                        // FIXME: hard coded number
                        _sequence = 128;
                        res.setSequence(_sequence);
                        byte[] crc = Ints.toByteArray(_sequence);
                        crc = gssContext.getMIC(crc, 0, 4, new MessageProp(false));
                        authGss.setVerifier(new RpcAuthVerifier(authGss.type(), crc));
                    }
                    call.reply(res);
                    break;
                case GssProc.RPCSEC_GSS_DESTROY:
                    gssContext = _gssSessionManager.destroyContext(authGss.getHandle());
                    validateVerifier(authGss, gssContext);
                    gssContext.dispose();
                    break;
                case GssProc.RPCSEC_GSS_DATA:
                    gssContext =  _gssSessionManager.getEstablishedContext(authGss.getHandle());
                    validateVerifier(authGss, gssContext);
                    GSSName sourceName = gssContext.getSrcName();
                    authGss.getSubject()
                            .getPrincipals()
                            .addAll(_gssSessionManager.subjectOf(sourceName).getPrincipals());
                    _log.debug("RPCGSS_SEC: {}",sourceName);
                    byte[] crc = Ints.toByteArray(authGss.getSequence());
                    crc = gssContext.getMIC(crc, 0, 4, new MessageProp(false));
                    authGss.setVerifier(new RpcAuthVerifier(authGss.type(), crc));
                    context.setAttribute(RpcProtocolFilter.RPC_CALL,
                            new RpcGssCall(call, gssContext, new MessageProp(false)));
                    hasContext = true;
            }

        } catch (RpcException e) {
            call.reject(e.getStatus(), e.getRpcReply());
            _log.info("GSS mechanism failed {}", e.getMessage());
        } catch (IOException e) {
            call.reject(RpcRejectStatus.AUTH_ERROR, new RpcAuthError(RpcAuthStat.RPCSEC_GSS_CTXPROBLEM));
            _log.info("GSS mechanism failed {}", e.getMessage());
        } catch (OncRpcException e) {
            call.reject(RpcRejectStatus.AUTH_ERROR, new RpcAuthError(RpcAuthStat.RPCSEC_GSS_CTXPROBLEM));
            _log.info("GSS mechanism failed {}", e.getMessage());
        } catch (GSSException e) {
            call.reject(RpcRejectStatus.AUTH_ERROR, new RpcAuthError(RpcAuthStat.RPCSEC_GSS_CTXPROBLEM));
            _log.info("GSS mechanism failed {}", e.getMessage());
        }
        return hasContext;
    }

    /**
     * According to rfc2203 verifier should contain the checksum of the RPC header
     * up to and including the credential.
     *
     * @param auth
     * @param context gss context
     * @throws GSSException if cant validate the checksum
     */
    private void validateVerifier(RpcAuthGss auth, GSSContext context) throws GSSException {
        ByteBuffer header = auth.getHeader();
        byte[] bb = new byte[header.remaining()];
        header.get(bb);
        context.verifyMIC(auth.getVerifier().getBody(), 0, auth.getVerifier().getBody().length,
                bb, 0, bb.length, new MessageProp(false));
    }

    @Override
    public boolean postExecute(Context cntxt) throws IOException {
        return true;
    }
}
