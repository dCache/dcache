package gplazma.authz.util;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.GSSConstants;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSContext;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Inet4Address;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Vector;

/**
 * HostUtil.java.
 * User: tdh
 * Date: Sep 29, 2008
 * Time: 10:42:05 AM
 * From dCache LoginManager code.
 */
public class HostUtil {

    public static String[] getHosts() throws SocketException {
        InetAddress[] addresses = getInetAddress();

        if( (addresses == null) || ( addresses.length == 0 ) ) return null;

        String[] hosts = new String[addresses.length];

        /**
         *  Add addresses ensuring preferred ordering: external addresses are before any
         *  internal interface addresses.
         */
        int nextExternalIfIndex = 0;
        int nextInternalIfIndex = addresses.length-1;

        for( int i = 0; i < addresses.length; i++) {
            InetAddress addr = addresses[i];

            if( !addr.isLinkLocalAddress() && !addr.isLoopbackAddress() &&
                    !addr.isSiteLocalAddress() && !addr.isMulticastAddress()) {
                hosts [nextExternalIfIndex++] = addr.getHostName();
            } else {
                hosts [nextInternalIfIndex--] = addr.getHostName();
            }
        }

        return hosts;
    }

    public static InetAddress[] getInetAddress() throws SocketException {
        InetAddress[] addresses = null;

        /**
         *  put all local Ip addresses, except loopback
         */

        Enumeration<NetworkInterface> ifList = NetworkInterface.getNetworkInterfaces();

        Vector<InetAddress> v = new Vector<InetAddress>();
        while( ifList.hasMoreElements() ) {

            NetworkInterface ne = ifList.nextElement();

            Enumeration<InetAddress> ipList = ne.getInetAddresses();
            while( ipList.hasMoreElements() ) {
                InetAddress ia = ipList.nextElement();
                // Currently we do not handle ipv6
                if( ! (ia instanceof Inet4Address) ) continue;
                if( ! ia.isLoopbackAddress() ) {
                    v.add( ia ) ;
                }
            }
        }
        addresses = v.toArray( new InetAddress[ v.size() ] );

        return addresses;
    }

    public static GSSContext getServiceContext() throws GSSException {
        return getServiceContext(X509CertUtil.default_service_cert, X509CertUtil.default_service_key, X509CertUtil.default_trusted_cacerts);
    }

    public static GSSContext getServiceContext(String service_cert, String service_key, String service_trusted_certs) throws GSSException {

        GlobusCredential serviceCredential;
        try {
            serviceCredential =new GlobusCredential(
                    service_cert,
                    service_key
                    );
        } catch(GlobusCredentialException gce) {
            throw new GSSException(GSSException.NO_CRED ,
                    0,
                    "could not load host globus credentials "+gce.toString());
        }


        GSSCredential cred = new GlobusGSSCredentialImpl(serviceCredential,
                GSSCredential.INITIATE_AND_ACCEPT);
        TrustedCertificates trusted_certs =
                TrustedCertificates.load(service_trusted_certs);
        GSSManager manager = ExtendedGSSManager.getInstance();
        ExtendedGSSContext context =
                (ExtendedGSSContext) manager.createContext(cred);

        context.setOption(GSSConstants.GSS_MODE,
                GSIConstants.MODE_GSI);
        context.setOption(GSSConstants.TRUSTED_CERTIFICATES,
                trusted_certs);

        return context;
    }

}
