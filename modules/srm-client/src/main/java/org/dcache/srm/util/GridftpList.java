/*
 * GridftpList.java
 *
 * Created on April 27, 2005, 1:13 PM
 */

package org.dcache.srm.util;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import java.net.URI;
import org.dcache.util.PortRange;
import org.dcache.util.URIs;

/**
 * @author timur
 */
public class GridftpList {


    /**
     * Creates a new instance of GridftpList
     */
    public GridftpList() {
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1 ||
              args[0].equalsIgnoreCase("-h") ||
              args[0].equalsIgnoreCase("-help") ||
              args[0].equalsIgnoreCase("--h") ||
              args[0].equalsIgnoreCase("--help")) {
            System.err.println(
                  "usage:\n" +
                        "       gridftplist < gridftp directory url> [<server passive (true or false)> \n"
                        +
                        "  example:" +
                        "       gridftplist gsiftp://host1:2811//dir1/dir-to-list ");

            System.exit(1);
            return;
        }
        String directory = args[0];
        boolean serverPassive = true;
        if (args.length > 1) {
            serverPassive = args[1].equalsIgnoreCase("true");
        }

        URI directory_url = new URI(directory);

        if (!directory_url.getScheme().equals("gsiftp") &&
              !directory_url.getScheme().equals("gridftp")) {
            System.err.println("wrong protocol : " + directory_url.getScheme());
            System.exit(1);
            return;
        }

        GridftpClient client = new GridftpClient(directory_url.getHost(),
              URIs.portWithDefault(directory_url), PortRange.getGlobusTcpPortRange(), null,
              new String[0],
              "/etc/grid-security/certificates",
              CrlCheckingMode.IF_VALID, NamespaceCheckingMode.EUGRIDPMA_GLOBUS,
              OCSPCheckingMode.IF_AVAILABLE);
        client.setStreamsNum(1);

        System.out.println(client.list(directory_url.getPath(), serverPassive));
        //for(java.util.Iterator i = paths.iterator(); i.hasNext();) {
        //    String next = (String)i.next();
        //    System.out.println(next);
        //}

        client.close();
    }


}
