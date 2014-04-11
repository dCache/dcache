/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.nearline.spi;

import java.net.URI;

/**
 * A request to remove a file from nearline storage.
 *
 * A remove request has no result value beyond success or failure to
 * delete the file.
 */
public interface RemoveRequest extends NearlineRequest<Void>
{
    /**
     * URI of the file to remove as generated when the file was flushed.
     *
     * @return URI of the file to remove
     */
    URI getUri();
}
