package org.dcache.boot;

import com.google.common.collect.Sets;

import java.util.Properties;
import java.util.Set;

import org.dcache.util.ConfigurationProperties;

import static org.dcache.boot.Properties.*;

class DcacheConfigurationUsageChecker implements ConfigurationProperties.UsageChecker
{
    private static final Set<String> GENERATED_PROPERTIES =
            Sets.newHashSet(
                    PROPERTY_DOMAINS,
                    PROPERTY_DOMAIN_NAME,
                    PROPERTY_DOMAIN_CELLS,
                    PROPERTY_DOMAIN_SERVICE,
                    PROPERTY_DOMAIN_SERVICE_BATCH,
                    PROPERTY_DCACHE_CONFIG_FILES,
                    PROPERTY_DCACHE_CONFIG_DIRS);

    @Override
    public boolean isStandardProperty(Properties defaults, String name)
    {
        return defaults.getProperty(name) != null || GENERATED_PROPERTIES.contains(name);
    }
}
