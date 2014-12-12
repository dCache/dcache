package org.dcache.util;

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
    private void buildMap()
    {
        ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();

        Replaceable replaceable = new Replaceable() {
            @Override
            public String getReplacement(String name)
            {
                Object value =  _environment.get(name);
                return (value == null) ? null : value.toString().trim();
            }
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
    public ImmutableMap<String, String> getObject() throws Exception
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
