package org.dcache.acl.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.dcache.acl.ACLException;
import org.dcache.acl.util.io.FileTools;

/**
 * Component which handles the generic module configuration.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class Config {

    // Logger
    private static final Logger _logger = Logger.getLogger("logger.org.dcache.authorization." + Config.class.getName());

    // Constants
    protected static final String SEPARATOR_COMMA = ", ";

    protected static final String CONN_DRIVER = "aclConnDriver";

    protected static final String CONN_URL = "aclConnUrl";

    protected static final String CONN_USER = "aclConnUser";

    protected static final String CONN_PSWD = "aclConnPswd";

    // Default constants
    private static final String DEFLT_USER = "";

    private static final String DEFLT_PSWD = "";

    // Protected member variables
    protected String _pswd;

    protected String _driver;

    protected String _url;

    protected String _user;

    protected String _cfile;

    protected Properties _props;

    public Config(String filename) throws ACLException {
        if (filename == null || filename.length() == 0)
            throw new IllegalArgumentException("Configuration file is undefined.");

        _cfile = filename;
        initProperties();
    }

    public Config(Properties props) throws ACLException {
        if (props == null)
            throw new IllegalArgumentException("Set of properties is undefined.");

        _props = props;
        initProperties();
    }

    // Methods
    protected void initProperties() throws ACLException {
        try {
            _driver = getProperties().getProperty(CONN_DRIVER);
            if ( _driver == null || _driver.length() == 0 )
                throw new MissingPropertyException(CONN_DRIVER, _cfile);

            _url = getProperties().getProperty(CONN_URL);
            if ( _url == null || _url.length() == 0 )
                throw new MissingPropertyException(CONN_URL, _cfile);

            _user = getProperties().getProperty(CONN_USER, DEFLT_USER);
            _pswd = getProperties().getProperty(CONN_PSWD, DEFLT_PSWD);

        } catch (PropertyException e) {
            throw new ACLException("Initialize Config", "PropertyException", e);

        } catch (IOException e) {
            throw new ACLException("Initialize Config", "IOException", e);

        } catch (Exception e) {
            throw new ACLException("Initialize Config", e);

        } finally {
            if ( _logger.isDebugEnabled() )
                _logger.debug(toString());
        }

    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(CONN_DRIVER).append(" = ").append(_driver).append(SEPARATOR_COMMA);
        sb.append(CONN_URL).append(" = ").append(_url).append(SEPARATOR_COMMA);
        sb.append(CONN_USER).append(" = ").append(_user).append(SEPARATOR_COMMA);
        sb.append(CONN_PSWD).append(" = ").append(_pswd == null ? null : "<hidden>");
        return sb.toString();
    }

    protected Boolean getBoolProperty(String key, Boolean deflt) throws IOException, PropertyException {
        final String value = getProperties().getProperty(key);
        try {
            return (value == null ? deflt : str2Bool(value));

        } catch (IllegalArgumentException e) {
            throw new InvalidPropertyException(key, value, _cfile);
        }
    }

    public static Boolean str2Bool(String str) throws IllegalArgumentException {
        if ( Boolean.TRUE.toString().equalsIgnoreCase(str) )
            return Boolean.TRUE;

        if ( Boolean.FALSE.toString().equalsIgnoreCase(str) )
            return Boolean.FALSE;

        throw new IllegalArgumentException("Invalid argument:" + str);
    }

    protected Integer getIntProperty(String key, Integer deflt) throws IOException, PropertyException {
        final String value = getProperties().getProperty(key);
        try {
            return (value == null ? deflt : Integer.valueOf(value));

        } catch (NumberFormatException e) {
            _logger.error("NumberFormatException: " + e.getMessage());
            throw new InvalidPropertyException(key, value, _cfile);
        }
    }

    protected String getFileProperty(String key) throws IOException, PropertyException {
        final String value = getProperties().getProperty(key);
        if ( value == null || value.length() == 0 )
            throw new MissingPropertyException(key, _cfile);

        try {
            FileTools.checkReadFile(new File(value));

        } catch (IOException e) {
            _logger.error("IOException: " + e.getMessage());
            throw new InvalidPropertyException(key, value, _cfile);
        }
        return value;
    }

    protected String getProperty(String key) throws IOException, PropertyException {
        final String value = getProperties().getProperty(key);
        if ( value == null || value.length() == 0 )
            throw new MissingPropertyException(key, _cfile);
        return value;
    }

    public Properties getProperties() throws IOException {
        if ( _props == null ) {
            File aFile = new File(_cfile);
            FileTools.checkReadFile(aFile);

            if ( _logger.isDebugEnabled() )
                _logger.debug("Loading configuration file: " + aFile);

            _props = new Properties();
            _props.load(new FileInputStream(aFile));
        }
        return _props;
    }

    public void setProperties(Properties properties) {
        _props = properties;
    }

    public String getConfigFile() {
        return _cfile;
    }

    public void setConfigFile(String cfile) {
        _cfile = cfile;
    }

    public String getDriver() {
        return _driver;
    }

    public void setDriver(String driver) {
        _driver = driver;
    }

    public String getUrl() {
        return _url;
    }

    public void setUrl(String url) {
        _url = url;
    }

    public String getUser() {
        return _user;
    }

    public void setUser(String user) {
        _user = user;
    }

    public void setPswd(String pswd) {
        _pswd = pswd;
    }

    public String getPswd() {
        return _pswd;
    }

}