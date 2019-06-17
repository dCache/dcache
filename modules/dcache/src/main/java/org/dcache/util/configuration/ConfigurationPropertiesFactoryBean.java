/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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

import org.springframework.beans.factory.FactoryBean;

import java.util.Properties;

/**
 * The ConfigurationPropertiesFactoryBean builds a Properties object from some
 * (possibly empty) subset of dCache configuration.  The Bean takes a String
 * prefix as an argument.  All configuration properties with a key that starts
 * with this prefix are used to build the properties object, all others are
 * ignored.  The entries are created by removing the prefix from matching
 * property keys to form the properties-entry's key.  The corresponding
 * properties-entry's value is the property value.
 * <p>
 * The created Properties bean is mutable.  However, the key-value pairs
 * represented by this Properties object are not backed by dCache configuration.
 * Any changes make to the created Properties bean are not reflected elsewhere
 * within dCache.
 */
public class ConfigurationPropertiesFactoryBean extends AbstractPrefixFactoryBean
        implements FactoryBean<Properties>
{
    @Override
    public Properties getObject() throws Exception
    {
        Properties p = new Properties();
        p.putAll(configuration());
        return p;
    }

    @Override
    public Class<?> getObjectType()
    {
        return Properties.class;
    }
}
