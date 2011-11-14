package org.dcache.acl.config;

import java.io.IOException;
import java.util.Properties;

import org.dcache.acl.ACLException;

/**
 * Component which handles the generic ACL module configuration.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class AclConfig extends Config {

    // Constants
    private static final String ACL_CONFIG = "acl.configuration";

    private static final String ACL_ENABLED = "aclEnabled";

    private static final String ACL_CACHE_SIZE = "aclCacheSize";

    private static final String ACL_TABLE = "aclTable";

    // Default constants
    private static final Boolean DEFLT_ACL_ENABLED = Boolean.TRUE;

    private static final Integer DEFLT_CACHE_SIZE = new Integer(0);

    private static final String DEFLT_ACL_TABLE = "t_acl";

    // Static member variables
    private Boolean _isAclEnabled;

    private Integer _cacheSize;

    private String _aclTable;

    // Constructor(s)
    /**
     * Default constructor used acl.properties file
     *
     * @throws ACLException
     */
    public AclConfig() throws ACLException {
        this(System.getProperty(ACL_CONFIG, "acl.properties"));
    }

    /**
     * @param filename -
     *            Configuration file
     * @throws ACLException
     */
    public AclConfig(String filename) throws ACLException {
        super(filename);
    }

    /**
     * @param props -
     *            set of ACL properties
     *
     * @throws ACLException
     */
    public AclConfig(Properties props) throws ACLException {
        super(props);
    }

    // Methods
    protected void initProperties() throws ACLException {
        try {
            _isAclEnabled = getBoolProperty(ACL_ENABLED, DEFLT_ACL_ENABLED);
            _cacheSize = getIntProperty(ACL_CACHE_SIZE, DEFLT_CACHE_SIZE);
            _aclTable = getProperties().getProperty(ACL_TABLE, DEFLT_ACL_TABLE);
            super.initProperties();

        } catch (PropertyException e) {
            throw new ACLException("Initialize ACLConfig", "PropertyException", e);

        } catch (IOException e) {
            throw new ACLException("Initialize ACLConfig", "IOException", e);

        } catch (Exception e) {
            throw new ACLException("Initialize ACLConfig", e);
        }

    }

    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(SEPARATOR_COMMA).append(ACL_ENABLED).append(" = ").append(_isAclEnabled);
        sb.append(SEPARATOR_COMMA).append(ACL_CACHE_SIZE).append(" = ").append(_cacheSize);
        sb.append(SEPARATOR_COMMA).append(ACL_TABLE).append(" = ").append(_aclTable);
        return sb.toString();
    }

    public boolean isAclEnabled() {
        return (_isAclEnabled == null ? DEFLT_ACL_ENABLED : _isAclEnabled);
    }

    public void setAclEnabled(Boolean aclEnabled) {
        _isAclEnabled = aclEnabled;
    }

    public Integer getCacheSize() {
        return _cacheSize;
    }

    public void setCacheSize(Integer cacheSize) {
        _cacheSize = cacheSize;
    }

    public String getACLTable() {
        return _aclTable;
    }

    public void setACLTable(String aclTable) {
        _aclTable = aclTable;
    }
}