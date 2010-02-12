package org.dcache.acl.client;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.acl.ACL;
import org.dcache.acl.handler.DefaultACLHandler;
import org.dcache.acl.parser.ACLParser;

/**
 * Manipulate access control lists.
 *
 * @author David Melkumyan, DESY Zeuthen
 * @version 0.0.1b
 */
public class SetAclClient extends AclClient {

    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + SetAclClient.class.getName());

    public SetAclClient(String[] args) {
        super(args);
    }

    public SetAclClient(String[] args, boolean stopAtNonOption) {
        super(args, stopAtNonOption);
    }

    /**
     * @param args
     *            contains the ACL
     */
    public static void main(String[] args) {

        SetAclClient app = new SetAclClient(args);
        try {
            if ( args.length == 0 )
                throw new MissingArgumentException("acl");

            ACL newACL = ACLParser.parse(args[0]);
            if ( newACL == null )
                throw new RuntimeException("SetAclClient failed: new ACL is NULL.");

//			if ( logger.isDebugEnabled() )
//				logger.debug("New ACL: " + newACL.toNFSv4String());

            final String rsId = newACL.getRsId();

            DefaultACLHandler aclHandler = new DefaultACLHandler();
            ACL oldACL = aclHandler.getACL(rsId);

            if ( oldACL != null ) {
//				if ( logger.isDebugEnabled() )
//					logger.debug("Old ACL: " + oldACL.toNFSv4String());

                aclHandler.removeACL(rsId);
                try {
                    aclHandler.setACL(newACL);

                } catch (Exception e) {
                    boolean ret = aclHandler.setACL(oldACL);
                    logger.info("Set new ACL failed. Rollback old ACL ..." + (ret ? "OK" : "FAILED"));
                    throw e;
                }

            } else
                aclHandler.setACL(newACL);

        } catch (MissingArgumentException mae) {
            app.exitHelp(1, "SetAclClient failed. Missing Argument Exception: " + mae.getMessage());

        } catch (IllegalArgumentException iae) {
            app.exitHelp(2, "SetAclClient failed. Illegal Argument Exception: " + iae.getMessage());

        } catch (Exception e) {
            logger.error("SetAclClient failed. Exception: " + e.getMessage());
            System.exit(3);
        }

    }

    void exitHelp(int exitValue) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("java " + getClass().getName() + " acl_spec", createOptions());
        System.exit(exitValue);
    }
}
