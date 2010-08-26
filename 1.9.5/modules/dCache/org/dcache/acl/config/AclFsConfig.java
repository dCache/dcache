package org.dcache.acl.config;

import java.util.Properties;

import org.dcache.acl.ACLException;

/**
 * Component which handles the file system context specific configuration.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class AclFsConfig extends AclConfig {

    /**
     * Default constructor used acl.properties file
     *
     * @throws ACLException
     */
    public AclFsConfig() throws ACLException {
        super();
    }

    /**
     * @param filename -
     *            Configuration file
     * @throws ACLException
     */
    public AclFsConfig(String filename) throws ACLException {
        super(filename);
    }

    /**
     * @param props -
     *            set of ACL properties
     *
     * @throws ACLException
     */
    public AclFsConfig(Properties props) throws ACLException {
        super(props);
    }

}
