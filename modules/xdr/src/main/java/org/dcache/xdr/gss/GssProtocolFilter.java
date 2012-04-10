package org.dcache.xdr.gss;

import com.google.common.primitives.Ints;
import java.io.IOException;
import java.util.UUID;
import org.dcache.utils.Bytes;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.RpcAuthError;
import org.dcache.xdr.RpcAuthStat;
import org.dcache.xdr.RpcAuthType;
import org.dcache.xdr.RpcAuthVerifier;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.RpcException;
import org.dcache.xdr.RpcRejectStatus;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
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
public class GssProtocolFilter extends BaseFilter {

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
    public NextAction handleRead(FilterChainContext ctx) throws IOException {

        RpcCall call = ctx.getMessage();

        if (call.getCredential().type() != RpcAuthType.RPCGSS_SEC) {
            return ctx.getInvokeAction();
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
                    ctx.setMessage(new RpcGssCall(call, gssContext, new MessageProp(false)));
                    hasContext = true;
            }

        } catch (RpcException e) {
            _log.warn("RPC error: {}", e.getMessage());
            call.reject(e.getStatus(), e.getRpcReply());
        } catch (IOException e) {
            _log.warn("IO Error: {}", e.getMessage());
            call.reject(RpcRejectStatus.AUTH_ERROR, new RpcAuthError(RpcAuthStat.RPCSEC_GSS_CTXPROBLEM));
        } catch (OncRpcException e) {
            _log.warn("RPC error: {}", e.getMessage());
            call.reject(RpcRejectStatus.AUTH_ERROR, new RpcAuthError(RpcAuthStat.RPCSEC_GSS_CTXPROBLEM));
        } catch (GSSException e) {
            _log.warn("GSS error: {}", e.getMessage());
            call.reject(RpcRejectStatus.AUTH_ERROR, new RpcAuthError(RpcAuthStat.RPCSEC_GSS_CTXPROBLEM));
        }

        if(hasContext)
            return ctx.getInvokeAction();

        return ctx.getStopAction();
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
        Buffer header = auth.getHeader();
        byte[] bb = new byte[header.remaining()];
        header.get(bb);
        context.verifyMIC(auth.getVerifier().getBody(), 0, auth.getVerifier().getBody().length,
                bb, 0, bb.length, new MessageProp(false));
    }
}
