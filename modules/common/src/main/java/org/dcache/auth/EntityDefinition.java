/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth;

/**
 * The EntityDefinition defines what kind of entity holds the private key
 * corresponding to this certificate.
 */
public enum EntityDefinition
{
    /**
     * A non-human automated client or robot.
     * @see <a href="https://www.eugridpma.org/guidelines/1scp/">IGTF
     * guidelines</a>.
     */
    ROBOT("Robot"),

    /**
     * A networked end-point entity (host).
     * @see <a href="https://www.eugridpma.org/guidelines/1scp/">IGTF
     * guidelines</a>.
     */
    HOST("Host"),

    /**
     * A natural person.
     * @see <a href="https://www.eugridpma.org/guidelines/1scp/">IGTF
     * guidelines</a>.
     */
    PERSON("Person");

    private final String name;

    EntityDefinition(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }
}
