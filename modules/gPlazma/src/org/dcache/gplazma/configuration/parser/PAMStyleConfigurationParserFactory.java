/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.gplazma.configuration.parser;

/**
 *
 * @author timur
 */
public class PAMStyleConfigurationParserFactory extends ConfigurationParserFactory {

    @Override
    public ConfigurationParser newConfigurationParser() {
        return new PAMStyleConfigurationParser();
    }

}
