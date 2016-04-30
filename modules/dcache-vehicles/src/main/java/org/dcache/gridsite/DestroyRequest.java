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
package org.dcache.gridsite;

import javax.security.auth.Subject;

import java.io.Serializable;

public class DestroyRequest implements Serializable
{
    private static final long serialVersionUID = -6038908629605914149L;
    private final Subject subject;
    private final String delegationID;

    public DestroyRequest(Subject subject, String delegationID)
    {
        this.subject = subject;
        this.delegationID = delegationID;
    }

    public String getDelegationID()
    {
        return delegationID;
    }

    public Subject getSubject()
    {
        return subject;
    }
}
