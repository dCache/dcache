package org.dcache.xdr.gss;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.dcache.xdr.RpcLoginService;
import org.dcache.utils.Opaque;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

public class GssSessionManager {

    private final GSSManager gManager = GSSManager.getInstance();
    private final GSSCredential _serviceCredential;
    private final RpcLoginService _loginService;

    public GssSessionManager(RpcLoginService loginService) throws GSSException {
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
        _serviceCredential = gManager.createCredential(null,
                GSSCredential.INDEFINITE_LIFETIME,
                krb5Mechanism, GSSCredential.ACCEPT_ONLY);
        _loginService = loginService;
    }
    private final Map<Opaque, GSSContext> sessions = new ConcurrentHashMap<>();

    public GSSContext createContext(byte[] handle) throws GSSException {
        GSSContext context = gManager.createContext(_serviceCredential);
        sessions.put(new Opaque(handle), context);
        return context;
    }

    public GSSContext getContext(byte[] handle) throws GSSException {
        GSSContext context = sessions.get(new Opaque(handle));
        if(context == null) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }
        return context;
    }
    public GSSContext getEstablishedContext(byte[] handle) throws GSSException {
        GSSContext context = getContext(handle);
        if (!context.isEstablished()) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }
        return context;
    }

    public GSSContext destroyContext(byte[] handle) throws GSSException {
        GSSContext context = sessions.remove(new Opaque(handle));
        if(context == null || !context.isEstablished()) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }
        return context;
    }

    public Subject subjectOf(GSSName name) {
        return _loginService.login(  new KerberosPrincipal(name.toString()));
    }
}
