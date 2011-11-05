package org.dcache.gplazma.configuration.parser;

/**
 *
 * @author timur
 */
public class ClassicConfigurationParserFactory extends ConfigurationParserFactory{

    @Override
    public ConfigurationParser newConfigurationParser() {
        return new ClassicConfigurationParser();
    }


}
