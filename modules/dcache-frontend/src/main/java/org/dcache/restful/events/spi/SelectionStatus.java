/*
 * dCache - http://www.dcache.org/
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
package org.dcache.restful.events.spi;

/**
 * The status of a request: either a selection or subscription request.
 */
public enum SelectionStatus
{
    /** A new item has been created. */
    CREATED,

    /**
     * The request was merged with an existing item.
     */
    MERGED,

    /**
     * The request targeted a dCache resource that does not exist.
     */
    RESOURCE_NOT_FOUND,

    /**
     * The request targeted a dCache resource that the user is not
     * allowed to access.
     */
    PERMISSION_DENIED,

    /**
     * The selector could not be accepted as it is badly formed or missing
     * required information.
     */
    BAD_SELECTOR,

    /**
     * Some internal error.
     */
    INTERNAL_ERROR
}
