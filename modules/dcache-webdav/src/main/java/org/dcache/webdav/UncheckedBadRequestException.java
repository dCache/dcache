/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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

import io.milton.resource.Resource;

/**
 * An exception thrown to indicate the server should return a 400 Bad Request
 * response to the client, but in a place where milton did not anticipate this
 * possibility.
 */
public class UncheckedBadRequestException extends WebDavException
{
    public UncheckedBadRequestException(Resource resource)
    {
        super(resource);
    }

    public UncheckedBadRequestException(String message, Resource resource)
    {
        super(message, resource);
    }

    public UncheckedBadRequestException(String message, Throwable cause, Resource resource)
    {
        super(message, cause, resource);
    }
}
