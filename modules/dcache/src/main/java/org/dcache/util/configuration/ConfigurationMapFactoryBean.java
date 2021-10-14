/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2019 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.util.configuration;

import java.util.Map;
import org.springframework.beans.factory.FactoryBean;


/**
 * The ConfigurationMapFactoryBean builds a Map from some (possibly empty) subset of dCache
 * configuration.  The Bean takes a String prefix as an argument.  All configuration properties with
 * a key that starts with this prefix are used to build the map, all others are ignored.  The map
 * entries are created by removing the prefix from matching property keys to form the map-entry's
 * key.  The corresponding map-entry's value is the property value.
 * <p>
 * The created Map bean is immutable.
 */
public class ConfigurationMapFactoryBean extends AbstractPrefixFactoryBean
      implements FactoryBean<Map<String, String>> {

    @Override
    public Map<String, String> getObject() {
        return configuration();
    }

    @Override
    public Class<?> getObjectType() {
        return Map.class;
    }
}
