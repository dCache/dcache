package org.dcache.boot;

/**
 * Constants for property names used by the boot loader.
 */
public class Properties
{
    public static final String PROPERTY_DCACHE_LAYOUT_URI = "dcache.layout.uri";
    public static final String PROPERTY_DCACHE_CONFIG_CACHE = "dcache.config.cache";
    public static final String PROPERTY_DCACHE_CONFIG_FILES = "dcache.config.files";
    public static final String PROPERTY_HOST_NAME = "host.name";
    public static final String PROPERTY_HOST_FQDN = "host.fqdn";
    public static final String PROPERTY_DOMAIN_NAME = "domain.name";
    public static final String PROPERTY_DOMAIN_SERVICE = "domain.service";
    public static final String PROPERTY_DOMAIN_CELLS = "domain.cells";
    public static final String PROPERTY_DOMAIN_SERVICE_URI_BASE = "domain.service.uri.base";
    public static final String PROPERTY_DOMAIN_SERVICE_URI = "domain.service.uri";
    public static final String PROPERTY_DOMAIN_PRELOAD = "domain.preload";
    public static final String PROPERTY_LOG_CONFIG = "dcache.log.configuration";

    public static final String PROPERTY_DOMAINS = "dcache.domains";
    public static final String PROPERTY_CELL_NAME = "cell.name";
    public static final String PROPERTY_PLUGIN_PATH = "dcache.paths.plugins";
    public static final String PROPERTY_DEFAULTS_PATH = "dcache.paths.defaults";
    public static final String PROPERTY_SETUP_PATH = "dcache.paths.setup";

    public static final String PATH_DELIMITER = ":";

    protected Properties()
    {
    }
}