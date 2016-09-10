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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STErrorListener;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.io.IOException;

import org.dcache.util.Slf4jSTErrorListener;

import static java.util.Objects.requireNonNull;

/**
 * This is a simple wrapper to allow reloading a template-group.
 */
public class ReloadableTemplate
{
    private static final Logger LOG = LoggerFactory.getLogger(ReloadableTemplate.class);

    private final Resource _resource;
    private final String _path;

    private STErrorListener _listener = new Slf4jSTErrorListener(LOG);
    private String _templateName;
    private STGroup _templateGroup;

    public ReloadableTemplate(Resource resource) throws IOException
    {
        _resource = requireNonNull(resource);
        _path = _resource.getFile().getCanonicalPath();
    }

    /**
     * Register a template name that should be instantiated when the
     * template is loaded.  This is a work-around for race conditions in
     * ST.
     */
    public void setTemplateName(String templateName)
    {
        _templateName = requireNonNull(templateName);
    }

    protected void reload() throws IOException
    {
        STGroupFile group = new STGroupFile(_resource.getURL(), "UTF-8", '$', '$');

        group.setListener(_listener);

        /* StringTemplate has lazy initialisation, but this is very racey and
         * can break StringTemplate altogether:
         *
         *     https://github.com/antlr/stringtemplate4/issues/61
         *
         * here we force initialisation to work-around this.
         */
        if (_templateName != null) {
            group.getInstanceOf(_templateName);
        }

        _templateGroup = group;
    }

    protected STGroup getTemplateGroup()
    {
        return _templateGroup;
    }

    public ST getInstanceOf(String name)
    {
        return getInstanceOf(name, false);
    }

    public ST getInstanceOfQuietly(String name)
    {
        return getInstanceOf(name, true);
    }

    private ST getInstanceOf(String name, boolean quiet)
    {
        ST template = null;

        STGroup group = getTemplateGroup();

        if (group != null) {
            template = group.getInstanceOf(name);

            if (template == null) {
                if (quiet) {
                    LOG.debug("template '{}' not found in templategroup: {}",
                            name, _path);
                } else {
                    LOG.error("template '{}' not found in templategroup: {}",
                            name, _path);
                }
            }
        }

        return template;
    }

    public String getPath()
    {
        return _path;
    }

    protected Resource getResource()
    {
        return _resource;
    }
}
