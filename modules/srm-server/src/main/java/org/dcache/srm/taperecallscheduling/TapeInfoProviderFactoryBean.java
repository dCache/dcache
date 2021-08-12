/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.srm.taperecallscheduling;

import org.dcache.srm.taperecallscheduling.spi.TapeInfoProviderProvider;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import java.util.NoSuchElementException;
import java.util.ServiceLoader;

/**
 * Spring factory bean to create SchedulingStrategyProviders.
 */
public class TapeInfoProviderFactoryBean implements FactoryBean<TapeInfoProviderProvider>
{
    private final ServiceLoader<TapeInfoProviderProvider> PROVIDERS = ServiceLoader.load(TapeInfoProviderProvider.class);
    private String name;
    private String tapeInfoDir;

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
    public void setTapeInfoDir(String directory)
    {
        tapeInfoDir = directory;
    }

    @Override
    public TapeInfoProviderProvider getObject() throws Exception
    {
        for (TapeInfoProviderProvider provider : PROVIDERS) {
            if (provider.getName().equals(name)) {
                provider.setFileDirectory(tapeInfoDir);
                return provider;
            }
        }
        throw new NoSuchElementException("No such tape info provider: " + name);
    }

    @Override
    public Class<?> getObjectType()
    {
        return TapeInfoProviderProvider.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }
}
