/*
 * Tools.java
 *
 * Created on February 24, 2004, 10:41 AM
 */

package org.dcache.srm.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
/**
 *
 * @author  timur
 */
public class Tools {

    /** Creates a new instance of Tools */
    public Tools() {
    }

    public static final boolean sameHost(String host1, String host2)
        throws UnknownHostException {
        InetAddress[] host1Addrs = InetAddress.getAllByName(host1);
        InetAddress[] host2Addrs = InetAddress.getAllByName(host2);
        for(InetAddress host1Addr: host1Addrs) {
            for(InetAddress host2Addr: host2Addrs) {
                if(host1Addr.equals(host2Addr)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static final boolean sameHost(Set<String> hosts, String host2 )
        throws UnknownHostException {
        for(String host1:hosts) {
            if(sameHost(host1,host2)) {
                return true;
            }
        }
        //System.out.println("sameHost("+host1+","+ host2+") returns false");
        return false;
    }

    private static final String[] emptyArr = new String[0];

    public static final String[] trimStringArray(String[] sarray) {

        if(sarray ==null) {
            return null;
        }
        List<String> protList = new ArrayList<>();
        for(String protocol:sarray) {
            if(protocol != null ) {
                protocol = protocol.trim();
                if(protocol.length() >0) {
                    protList.add(protocol);
                }
            }
        }
        return protList.toArray(new String[protList.size()]);
    }

}
