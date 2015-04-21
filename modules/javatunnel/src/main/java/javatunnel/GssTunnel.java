/*
 * $Id: GssTunnel.java,v 1.7 2006-10-11 09:49:58 tigran Exp $
 */

package javatunnel;

import org.ietf.jgss.ChannelBinding;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


class GssTunnel extends TunnelConverter {

    private final static Logger _log = LoggerFactory.getLogger(GssTunnel.class);

    // GSS Kerberos context and others
    private GSSManager _gManager;
    protected GSSContext _context;
    private GSSCredential _myCredential;
    private GSSCredential _userCredential;
    protected GSSName _userPrincipal;
    protected GSSName _myPrincipal;
    protected GSSName _peerPrincipal;
    private boolean _authDone;
    MessageProp _prop =  new MessageProp(true);
    private String _principalStr;
    private boolean _useChannelBinding = true;

    protected GssTunnel() {}

    // Creates a new instance of GssTunnel
    public GssTunnel(String principalStr ) {
    	_principalStr   = principalStr;
    }

    public GssTunnel(String principalStr, boolean init) throws GSSException {


        if ( init) {
            Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");

            _gManager = GSSManager.getInstance();
            _myPrincipal = _gManager.createName(principalStr, null);

            _myCredential = _gManager.createCredential(_myPrincipal,
                    GSSCredential.INDEFINITE_LIFETIME,
                    krb5Mechanism,
                    GSSCredential.ACCEPT_ONLY);

            _context = _gManager.createContext(_myCredential);
        }
   }

    public GssTunnel(String principalStr, String peerName) throws GSSException {

        Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");

        _gManager = GSSManager.getInstance();
        _myPrincipal = _gManager.createName(principalStr, null);

        _myCredential = _gManager.createCredential(_myPrincipal,
                GSSCredential.DEFAULT_LIFETIME,
                krb5Mechanism,
                GSSCredential.INITIATE_ONLY);

        _peerPrincipal = _gManager.createName(peerName, null);

        _context = _gManager.createContext(_peerPrincipal,
                krb5Mechanism,
                _myCredential,
                GSSContext.DEFAULT_LIFETIME);
    }

    // subclasses allowed to disable cannel binding
    protected void useChannelBinding(boolean use) {
    	_useChannelBinding = use;
    }

    @Override
    public byte[] decode(InputStream in) throws IOException {
        byte[] retValue;

        retValue = super.decode(in);

        if(_authDone) {
            try {
                retValue = _context.unwrap(retValue, 0, retValue.length, _prop);
            } catch (GSSException ge) {
                throw new IOException("Failed to unwrap message: " + ge.getMessage());
            }
        }

        return retValue;
    }

    @Override
    public void encode(byte[] buf, int len, OutputStream out) throws IOException {
        byte[] nb;
        int nlen;


        if(_authDone) {
            try{
                nb = _context.wrap(buf, 0, len, _prop);
            } catch (GSSException ge) {
                throw new IOException(ge.getMessage());
            }
            nlen = nb.length;
        }else{
            nb = buf;
            nlen = len;
        }
        super.encode(nb, nlen, out);
    }

    @Override
    public boolean auth( InputStream in, OutputStream out, Object addon) {

        boolean established = false;

        try {

            byte[] inToken = new byte[0];
            byte[] outToken;
            Socket socket = (Socket)addon;

            _context.requestMutualAuth(true);

            if( _useChannelBinding ) {
                 ChannelBinding cb = new ChannelBinding(socket.getInetAddress(), socket.getLocalAddress(), null);
            	_context.setChannelBinding(cb);
            }

            while(!established) {
                outToken = _context.initSecContext(inToken, 0, inToken.length);

                if( outToken != null) {
                    this.encode(outToken, outToken.length, out);
                }

                if( !_context.isEstablished() ) {
                    inToken = this.decode(in);
                }else {
                    established = true;
                }

            }

        } catch( Exception e) {
            _log.error("Failed to authenticate", e);
        }

        _authDone = established ;
        return established;
    }


    @Override
    public boolean verify( InputStream in, OutputStream out, Object addon) {

        try {

            byte[] inToken;
            byte[] outToken;
            Socket  socket = (Socket)addon;

            if(_useChannelBinding) {
                ChannelBinding cb = new ChannelBinding(socket.getInetAddress(),   socket.getLocalAddress(), null);
                _context.setChannelBinding(cb);
            }

            while(  !_context.isEstablished() ) {

                inToken = this.decode(in);

                outToken = _context.acceptSecContext(inToken, 0, inToken.length);

                if( outToken != null) {
                    this.encode(outToken, outToken.length, out);
                }

            }

            _userPrincipal = _context.getSrcName();

        } catch( EOFException e) {
            _log.debug("connection closed");
        } catch( IOException | GSSException e) {
            _log.error("Failed to verify: {}", e.toString());
        }

        _authDone = _context.isEstablished();
        return _authDone;
    }


    @Override
    public Subject getSubject() {
        Set<Principal> pricipals = new HashSet<>();
        try {
            pricipals.add( new KerberosPrincipal(_context.getSrcName().toString()) );
        }catch(GSSException e) {
            _log.error("Failed to create a kerberos principal:", e);
        }
        return  new Subject(false,  pricipals , Collections
                .emptySet(), Collections
                .emptySet());
    }

    @Override
    public Convertable makeCopy() throws IOException {
        try {
            return new GssTunnel( _principalStr, true);
        } catch (GSSException e) {
            throw new IOException(e);
        }
    }

}
