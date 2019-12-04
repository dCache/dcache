package org.dcache.boot;

import org.dcache.util.configuration.ConfigurationProperties;

/**
 * Constants for property names used by the boot loader.
 */
public class Properties
{
    public static final String PROPERTY_DCACHE_LAYOUT_URI = "dcache.layout.uri";
    public static final String PROPERTY_DCACHE_CONFIG_CACHE = "dcache.config.cache";
    public static final String PROPERTY_DCACHE_CONFIG_FILES = "dcache.config.files";
    public static final String PROPERTY_DCACHE_CONFIG_DIRS = "dcache.config.dirs";
    public static final String PROPERTY_DCACHE_VERSION = "dcache.version";
    public static final String PROPERTY_DCACHE_SCM_STATE = "dcache.scm-state";
    public static final String PROPERTY_HOST_NAME = "host.name";
    public static final String PROPERTY_HOST_FQDN = "host.fqdn";
    public static final String PROPERTY_DOMAIN_NAME = "dcache.domain.name";
    public static final String PROPERTY_DOMAIN_SERVICE = "dcache.domain.service";
    public static final String PROPERTY_DOMAIN_CELLS = "dcache.domain.cells";
    public static final String PROPERTY_DOMAIN_SERVICE_URI_BASE = "dcache.domain.service.uri.base";
    public static final String PROPERTY_DOMAIN_SERVICE_URI = "dcache.domain.service.uri";
    public static final String PROPERTY_DOMAIN_SERVICE_BATCH = "dcache.domain.service.batch";
    public static final String PROPERTY_DOMAIN_PRELOAD = "dcache.domain.preload";
    public static final String PROPERTY_LOG_CONFIG = "dcache.log.configuration";
    public static final String PROPERTY_ZONE = "dcache.zone";

    public static final String PROPERTY_ZOOKEPER_CONNECTION = "dcache.zookeeper.connection";
    public static final String PROPERTY_ZOOKEPER_RETRIES = "dcache.zookeeper.max-retries";
    public static final String PROPERTY_ZOOKEPER_SLEEP = "dcache.zookeeper.initial-retry-delay";
    public static final String PROPERTY_ZOOKEPER_SLEEP_UNIT = "dcache.zookeeper.initial-retry-delay.unit";
    public static final String PROPERTY_ZOOKEPER_CONNECTION_TIMEOUT = "dcache.zookeeper.connection-timeout";
    public static final String PROPERTY_ZOOKEPER_CONNECTION_TIMEOUT_UNIT = "dcache.zookeeper.connection-timeout.unit";
    public static final String PROPERTY_ZOOKEPER_SESSION_TIMEOUT = "dcache.zookeeper.session-timeout";
    public static final String PROPERTY_ZOOKEPER_SESSION_TIMEOUT_UNIT = "dcache.zookeeper.session-timeout.unit";

    public static final String PROPERTY_DOMAINS = "dcache.domains";
    public static final String PROPERTY_PLUGIN_PATH = "dcache.paths.plugins";
    public static final String PROPERTY_DEFAULTS_PATH = "dcache.paths.defaults";
    public static final String PROPERTY_SETUP_PATH = "dcache.paths.setup";

    public static final String PROPERTY_CELL_NAME_SUFFIX = "cell.name";

    public static final String PATH_DELIMITER = ":";

    protected Properties()
    {
    }

    public static String getCellName(ConfigurationProperties properties)
    {
        String serviceType = properties.getValue(PROPERTY_DOMAIN_SERVICE);
        return properties.getValue(serviceType + "." + PROPERTY_CELL_NAME_SUFFIX);
    }
}
