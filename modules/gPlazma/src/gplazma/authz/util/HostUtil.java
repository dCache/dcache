package gplazma.authz.util;

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
}
