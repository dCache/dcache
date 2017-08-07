/*
 * dCache - http://www.dcache.org/
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
package org.dcache.restful;

import io.swagger.annotations.BasicAuthDefinition;
import io.swagger.annotations.Contact;
import io.swagger.annotations.ExternalDocs;
import io.swagger.annotations.Info;
import io.swagger.annotations.License;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import io.swagger.annotations.Tag;

import javax.ws.rs.ext.Provider;

/**
 * This interface exists only to give @SwaggerDefinition something to annotate
 * that swagger will subsequently find.  See:
 *
 *     https://github.com/swagger-api/swagger-core/issues/2600
 */
@SwaggerDefinition(
        info = @Info(
            title = "The dCache REST interface",
            description = "User and administration interaction with dCache",
            version = "v1.0.1",
            contact = @Contact(
               name = "dCache support team",
               email = "support@dCache.org"
            ),
            license = @License(
                name = "Apache 2.0",
                url = "http://www.apache.org/licenses/LICENSE-2.0"
            )
        ),
        securityDefinition = @SecurityDefinition(
                basicAuthDefinitions = {
                    @BasicAuthDefinition(key = "basicAuth",
                            description = "Username and password authentication "
                                    + "with optional role assertion.  To assert "
                                    + "roles, append '#' to the username followed "
                                    + "by a comma-separated list of roles; e.g., "
                                    + "a username of \"paul#admin\" is user "
                                    + "\"paul\" asserting the \"admin\" role.")
                }
        ),
        consumes = {"application/json"},
        produces = {"application/json"},
        externalDocs = @ExternalDocs(value = "Wiki", url = "https://github.com/dCache/dcache/wiki/Restful-API"),
        tags = {
            @Tag(name = "alarms", description = "The log of internal problems"),
            @Tag(name = "billing", description = "The log of (significant) client activity"),
            @Tag(name = "cells", description = "The running components within dCache"),
            @Tag(name = "identity", description = "Information about users"),
            @Tag(name = "namespace", description = "Files, directories and similar objects"),
            @Tag(name = "poolmanager", description = "Data placement and selection decisions"),
            @Tag(name = "pools", description = "File data storage"),
            @Tag(name = "qos", description = "Managing how data is stored and handled"),
            @Tag(name = "spacemanager", description = "Ensuring enough capacity for uploads"),
            @Tag(name = "transfers", description = "The movement of data between dCache and clients"),
            @Tag(name = "events", description = "Support for SSE clients receiving dCache events")
        }
)
@Provider
public interface ApiConfig
{

}
