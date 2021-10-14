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

import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Command;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Callable;
import org.springframework.core.io.Resource;

/**
 * This is a simple wrapper to allow support admin-triggered reloading of a template-group.
 */
public class AdminReloadingTemplate extends ReloadableTemplate implements CellCommandListener {

    public AdminReloadingTemplate(Resource resource) throws IOException {
        super(resource);
    }

    @Command(name = "reload template", hint = "refresh HTML template from file")
    class ReloadTemplateCommand implements Callable<Serializable> {

        @Override
        public Serializable call() throws IOException {
            reload();
            return "";
        }
    }
}
