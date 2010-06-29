package org.dcache.gplazma.configuration;

/**
 * Configuration that never changes
 * @author timur
 */
public class StaticContentConfigurationLoadingStrategy
        implements  ConfigurationLoadingStrategy {


    private final Configuration configuration;

    public StaticContentConfigurationLoadingStrategy(
             Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public boolean hasUpdated() {
        return false;
    }

    @Override
    public Configuration load() {
        return configuration;
    }

}
