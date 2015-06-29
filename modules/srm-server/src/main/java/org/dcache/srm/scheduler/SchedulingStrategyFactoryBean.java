/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.srm.scheduler;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;

import org.dcache.srm.scheduler.spi.SchedulingStrategyProvider;

/**
 * Spring factory bean to create SchedulingStrategyProviders.
 */
public class SchedulingStrategyFactoryBean implements FactoryBean<SchedulingStrategyProvider>
{
    private final ServiceLoader<SchedulingStrategyProvider> PROVIDERS =
            ServiceLoader.load(SchedulingStrategyProvider.class);
    private String name;
    private Map<String, String> configuration;

    public String getName()
    {
        return name;
    }

    @Required
    public void setName(String name)
    {
        this.name = name;
    }

    @Required
    public void setConfiguration(Map<String, String> configuration)
    {
        this.configuration = configuration;
    }

    @Override
    public SchedulingStrategyProvider getObject() throws Exception
    {
        for (SchedulingStrategyProvider provider : PROVIDERS) {
            if (provider.getName().equals(name)) {
                provider.setConfiguration(configuration);
                return provider;
            }
        }
        throw new NoSuchElementException("No such scheduling strategy: " + name);
    }

    @Override
    public Class<?> getObjectType()
    {
        return SchedulingStrategyProvider.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }
}
