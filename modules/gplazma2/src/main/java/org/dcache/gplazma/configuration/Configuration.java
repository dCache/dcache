package org.dcache.gplazma.configuration;

import java.util.Collections;
import java.util.List;

/**
 *
 * @author timur
 */
public class Configuration {
    private List<ConfigurationItem> configurationItemList;
    public Configuration(List<ConfigurationItem> configurationItemList) {
        this.configurationItemList = configurationItemList;
    }

    public List<ConfigurationItem> getConfigurationItemList() {
        return Collections.unmodifiableList(configurationItemList);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(
                "# GPlazma 2.0 PAM Style configuration\n");
        for(ConfigurationItem configItem:configurationItemList) {
            sb.append(configItem).append('\n');
        }
        return sb.toString();
    }
}
