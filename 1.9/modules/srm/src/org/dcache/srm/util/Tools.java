/*
 * Tools.java
 *
 * Created on February 24, 2004, 10:41 AM
 */

package org.dcache.srm.util;
import java.net.InetAddress;
import java.util.List;
import java.util.ArrayList;
/**
 *
 * @author  timur
 */
public class Tools {
    
    /** Creates a new instance of Tools */
    public Tools() {
    }
    
    public static final boolean sameHost(String host1, String host2) 
        throws java.net.UnknownHostException {
        //System.out.println("sameHost("+host1+","+ host2+")");
        InetAddress[] host1_addrs = InetAddress.getAllByName(host1);
        InetAddress[] host2_addrs = InetAddress.getAllByName(host2);
        
        for(int indx1 = 0; indx1 < host1_addrs.length ; ++indx1) {
            for(int indx2 = 0; indx2 < host2_addrs.length ; ++indx2) {
               //System.out.println("sameHost("+host1+","+ host2+") comparing "+
               //host1_addrs[indx1]+" and "+host2_addrs[indx2]);

                if(host1_addrs[indx1].equals(host2_addrs[indx2])) {
                //System.out.println("sameHost("+host1+","+ host2+") returns true");
                    return true;
                }
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
        List<String> protList = new ArrayList<String>();
        for(String protocol:sarray) {
            if(protocol != null ) {
                protocol = protocol.trim();
                if(protocol.length() >0) {
                    protList.add(protocol);
                }
            }
        }
        return protList.toArray(emptyArr);
    }
    
}
