/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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

import java.io.Serializable;
import java.security.Principal;

/**
 * The presence of this principal indicates that the user is exempt from the normal namespace
 * permission rules.  The {@link org.dcache.auth.attributes.Restriction} accompanying a namespace
 * request is still enforced.  Code that inserts this principal should (very likely) also add
 * restrictions, otherwise the user will have root-like authority.
 */
@AuthenticationOutput
public class ExemptFromNamespaceChecks implements Principal, Serializable {

    @Override
    public String getName() {
        return "full"; // all namespace checks are by-passed.
    }

    @Override
    public String toString() {
        return "ExemptFromNamespaceChecks";
    }
}
