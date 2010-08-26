/**
 *
 */
package org.dcache.acl.client;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.log4j.Logger;
import org.dcache.acl.ACL;
import org.dcache.acl.handler.DefaultACLHandler;

/**
 * Get access control lists.
 *
 * @author David Melkumyan, DESY Zeuthen
 * @version 0.0.1b
 */
public class GetAclClient extends AclClient {

    // private final static String cellArgs =
    // " -acl-permission-handler-config=conf/acl.properties" +
    // "
    // -meta-data-provider=org.dcache.tests.namespace.FileMetaDataProviderHelper";
    //
    // private final static CellAdapterHelper _dummyCell = new
    // CellAdapterHelper("aclTestCell", cellArgs) ;
    // private final FileMetaDataProviderHelper _metaDataSource = new
    // FileMetaDataProviderHelper(_dummyCell);

    private static final Logger logger = Logger.getLogger("logger.org.dcache.authorization." + GetAclClient.class.getName());

    public GetAclClient(String[] args) {
        super(args);
    }

    public GetAclClient(String[] args, boolean stopAtNonOption) {
        super(args, stopAtNonOption);
    }

    /**
     * @param args
     *            contains the path/rsID of the resource
     */
    public static void main(String[] args) {
        GetAclClient app = new GetAclClient(args);
        try {
            if ( args.length == 0 )
                throw new MissingArgumentException("path");

            final String path = args[0];
            if ( path.length() == 0 )
                throw new IllegalArgumentException("path is Empty");

            final String rsId = app.fpath2rsId(path);
            if ( rsId == null || rsId.length() == 0 )
                throw new RuntimeException("Get rsId failure.");

            ACL acl = (new DefaultACLHandler()).getACL(rsId);
            if ( acl != null )
                logger.info(acl.toNFSv4String());

        } catch (MissingArgumentException mae) {
            app.exitHelp(1, "GetAclClient failed. Missing Argument Exception: " + mae.getMessage());

        } catch (IllegalArgumentException iae) {
            app.exitHelp(2, "GetAclClient failed. Illegal Argument Exception: " + iae.getMessage());

        } catch (Exception e) {
            logger.fatal("GetAclClient failed. Exception: " + e.getMessage());
            System.exit(3);
        }
    }

    void exitHelp(int exitValue) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("java " + getClass().getName() + " path", createOptions());
        System.exit(exitValue);
    }
}
