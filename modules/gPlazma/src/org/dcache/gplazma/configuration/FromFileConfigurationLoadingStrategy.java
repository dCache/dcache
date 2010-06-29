package org.dcache.gplazma.configuration;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.gplazma.configuration.parser.ConfigurationParser;
import org.dcache.gplazma.configuration.parser.ConfigurationParserFactory;

/**
 * This loading strategy loads the configuration from file, if file has been
 * updated.
 * This class is not thread safe.
 * @author timur
 */
public class FromFileConfigurationLoadingStrategy
        implements  ConfigurationLoadingStrategy {

    private static final long CONFIGURATION_UPDATE_FREQUNCY_MILLIS =
            TimeUnit.SECONDS.toMillis(1);

    private static final Logger LOGGER =
        LoggerFactory.getLogger( FromFileConfigurationLoadingStrategy.class);

    private final File configurationFile;
    private long configurationFileLastModified;
    private long configurationFileLastChecked;

    public FromFileConfigurationLoadingStrategy(File configurationFile) {
        if(!configurationFile.canRead()) {
            throw new IllegalArgumentException(
                    "can not read configuration " +
                    "from file "+configurationFile);
        }
        this.configurationFile = configurationFile;
    }

    public FromFileConfigurationLoadingStrategy(String configurationFileName) {
        this(new File(configurationFileName));
    }

    /**
     *
     * @return true if the configuration file has been modified,
     *   false otherwise
     */
    @Override
    public boolean hasUpdated() {
        if( (System.currentTimeMillis() - configurationFileLastChecked) <
                CONFIGURATION_UPDATE_FREQUNCY_MILLIS) {
            // we checked less then CONFIGURATION_UPDATE_FREQUNCY_MILLIS millis ago
            return false;
        }
        if(configurationFileLastModified == configurationFile.lastModified() ) {
            //configuration file has not been updated. no need to reread
            return false;
        }
        return true;
     }

    /**
     *
     * @return configuration loaded from the configuration file
     */
    @Override
    public Configuration load() {
        configurationFileLastModified =  configurationFile.lastModified();
        configurationFileLastChecked = System.currentTimeMillis();
        ConfigurationParserFactory parserFactory =
                ConfigurationParserFactory.getInstanse();
        ConfigurationParser parser = parserFactory.newConfigurationParser();
        return parser.parse(configurationFile);
    }
}
