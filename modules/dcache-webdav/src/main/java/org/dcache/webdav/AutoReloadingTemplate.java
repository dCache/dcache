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
import org.stringtemplate.v4.STErrorListener;
import org.stringtemplate.v4.STGroup;

import java.io.IOException;

/**
 * This is a simple wrapper to allow support auto reloading of a
 * template-group.
 */
public class AutoReloadingTemplate extends ReloadableTemplate
{
    private static final Logger LOG = LoggerFactory.getLogger(AutoReloadingTemplate.class);
    private static final long CHECK_PERIOD = 1_000;

    private long _lastModified;
    private long _lastChecked;

    public AutoReloadingTemplate(Resource resource)
            throws IOException
    {
        super(resource);
    }

    @Override
    protected synchronized STGroup getTemplateGroup()
    {
        if (System.currentTimeMillis() - _lastChecked > CHECK_PERIOD) {
            try {
                long lastModified = getResource().lastModified();

                if (_lastModified != lastModified) {
                    reload();
                    _lastModified = lastModified;
                }
            } catch (IOException e) {
                LOG.warn("Problem with template file {}: {}", getPath(), e.toString());
            }
        }

        return super.getTemplateGroup();
    }
}
