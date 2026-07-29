/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2001 - 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.util;

import java.util.Date;
import javax.ws.rs.core.Response;

public class Responses {

    public static Response buildResponse(Object result) {
        return buildResponse(result, System.currentTimeMillis());
    }

    public static Response buildResponse(Object result, long lastUpdated) {
        Response.ResponseBuilder builder = Response.ok(result);
        if (lastUpdated != 0) {
            builder.lastModified(new Date(lastUpdated));
        }
        return builder.build();
    }

    private Responses() {}
}
