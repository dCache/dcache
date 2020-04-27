/*
 * Copyright 1999-2010 University of Chicago
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS,WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.globus.gsi.gssapi.jaas;

import java.io.Serializable;
import java.security.Principal;

/**
 * Simple string-based principal.
 * @since 2.14
 */
public class SimplePrincipal
        implements Principal, Serializable
{
    private static final long serialVersionUID = 1495389510845512535L;
    private String name;

    public SimplePrincipal()
    {
    }

    public SimplePrincipal(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return this.name;
    }

    public int hashCode()
    {
        return (this.name == null) ? 0 : this.name.hashCode();
    }

    public boolean equals(Object another)
    {
        if (!(another instanceof Principal)) {
            return false;
        }
        String anotherName = ((Principal) another).getName();
        return this.name == null ? anotherName == null : this.name.equals(anotherName);
    }

    public String toString()
    {
        return getName();
    }

}
