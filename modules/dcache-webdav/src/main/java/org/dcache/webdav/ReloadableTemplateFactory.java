/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.webdav;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.io.Resource;

import org.dcache.cells.UniversalSpringCell.BeanPostProcessorAware;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating the reloadable template.
 */
public class ReloadableTemplateFactory implements FactoryBean, BeanPostProcessorAware
{
    private Resource _templateResource;
    private boolean _isAutoReloadEnabled;
    private String _templateName;
    private BeanPostProcessor _processor;

    @Override
    public Object getObject() throws Exception
    {
        ReloadableTemplate t = _isAutoReloadEnabled
                ? new AutoReloadingTemplate(_templateResource)
                : new AdminReloadingTemplate(_templateResource);
        t = (ReloadableTemplate) _processor.postProcessBeforeInitialization(t, _templateName);
        t.setTemplateName(_templateName);
        return t;
    }

    @Override
    public Class getObjectType()
    {
        return ReloadableTemplate.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }

    @Required
    public void setResource(Resource resource)
    {
        _templateResource = requireNonNull(resource);
    }

    @Required
    public void setAutoReload(boolean isEnabled)
    {
        _isAutoReloadEnabled = isEnabled;
    }

    @Required
    public void setWorkaroundTemplate(String name)
    {
        _templateName = requireNonNull(name);
    }

    @Override
    public void setBeanPostProcessor(BeanPostProcessor processor)
    {
        _processor = processor;
    }
}
