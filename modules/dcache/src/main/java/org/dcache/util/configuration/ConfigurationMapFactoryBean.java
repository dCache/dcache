/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2018 Deutsches Elektronen-Synchrotron
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

import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;

import java.util.Map;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The ConfigurationMapFactoryBean builds a Map from some (possibly empty)
 * subset of dCache configuration.  The Bean takes a String prefix as an
 * argument.  All configuration properties with a key that starts with this
 * prefix are used to build the map, all others are ignored.  The map entries
 * are created by removing the prefix from matching property keys to form the
 * map-entry's key.  The corresponding map-entry's value is the property value.
 */
public class ConfigurationMapFactoryBean implements EnvironmentAware,
        FactoryBean<ImmutableMap<String,String>>
{
    private String _prefix;
    private Map<String,Object> _environment;
    private ImmutableMap<String,String> _object;

    @Override
    public void setEnvironment(Map<String, Object> environment)
    {
        _environment = environment;
    }

    @Required
    public void setPrefix(String value)
    {
        _prefix = checkNotNull(value) + ConfigurationProperties.PREFIX_SEPARATOR;
    }

    @PostConstruct
    public void buildMap()
    {
        ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();

        Replaceable replaceable = name -> {
            Object value =  _environment.get(name);
            return (value == null) ? null : value.toString().trim();
        };

        int prefixLength = _prefix.length();
        for (Map.Entry<String,Object> item : _environment.entrySet()) {
            String name = item.getKey();
            if (item.getValue() instanceof String && name.startsWith(_prefix)) {
                String value = (String) item.getValue();
                String key = name.substring(prefixLength);
                if (!key.isEmpty()) {
                    builder.put(key, Formats.replaceKeywords(value, replaceable));
                }
            }
        }

        _object = builder.build();
    }

    @Override
    public ImmutableMap<String, String> getObject()
    {
        return _object;
    }

    @Override
    public Class<?> getObjectType()
    {
        return ImmutableMap.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }
}
