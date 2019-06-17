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

import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;

import java.util.Collections;
import java.util.Map;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * A base class for creating dictionary-like objects from dCache configuration.
 */
public abstract class AbstractPrefixFactoryBean implements EnvironmentAware
{
    private String _prefix;
    private Map<String,Object> _environment;
    private Map<String,String> _staticEnvironment = Collections.emptyMap();
    private ImmutableMap<String,String> _object;

    @Override
    public void setEnvironment(Map<String, Object> environment)
    {
        _environment = requireNonNull(environment);
    }

    public void setStaticEnvironment(Map<String, String> staticEnvironment)
    {
        _staticEnvironment = requireNonNull(staticEnvironment);
    }

    @Required
    public void setPrefix(String value)
    {
        _prefix = requireNonNull(value) + ConfigurationProperties.PREFIX_SEPARATOR;
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
            Object value = item.getValue();
            if (value instanceof String && name.startsWith(_prefix)) {
                String key = name.substring(prefixLength);
                if (!key.isEmpty() && !_staticEnvironment.containsKey(key)) {
                    builder.put(key, Formats.replaceKeywords(String.valueOf(value), replaceable));
                }
            }
        }

        builder.putAll(_staticEnvironment);

        _object = builder.build();
    }

    protected ImmutableMap<String,String> configuration()
    {
        checkState(_object != null, "buildMap not called");
        return _object;
    }
}
