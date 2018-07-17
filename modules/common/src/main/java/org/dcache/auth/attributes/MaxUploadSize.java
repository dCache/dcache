/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth.attributes;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A login attribute that indicates this user may write files less than or equal
 * to some specific file size.
 */
public class MaxUploadSize implements LoginAttribute, Serializable
{
    private static final long serialVersionUID = 1L;

    private final long maximumSize;

    public MaxUploadSize(long maximumSize)
    {
        checkArgument(maximumSize > 0, "max upload size must be positive");
        this.maximumSize = maximumSize;
    }

    public long getMaximumSize()
    {
        return maximumSize;
    }
}
