/*
 * dCache - http://www.dcache.org/
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
package org.dcache.srm;

import java.io.Serializable;

/**
 * SRM 2.2 response between frontend and backend.
 */
public class SrmResponse implements Serializable {

    private static final long serialVersionUID = -6879368676630818805L;
    private final String id;
    private final Object response;

    public SrmResponse(String id, Object response) {
        this.id = id;
        this.response = response;
    }

    public String getId() {
        return id;
    }

    public Object getResponse() {
        return response;
    }
}
