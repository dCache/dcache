/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

/**
 * Thrown when a remote service, e.g. pool manager, cannot be accessed. Replaces the exception of
 * the same name that Spring removed in version 6.
 */
public class RemoteAccessException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RemoteAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
