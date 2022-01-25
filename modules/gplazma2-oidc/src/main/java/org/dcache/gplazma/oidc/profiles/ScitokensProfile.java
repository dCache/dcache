/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.oidc.profiles;

import com.google.common.base.Splitter;
import diskCacheV111.util.FsPath;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.dcache.gplazma.AuthenticationException;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

public class ScitokensProfile extends ScopeBasedAuthzProfile {

    public ScitokensProfile(FsPath prefix) {
        super(prefix, Collections.emptySet(), Collections.emptySet());
    }

    @Override
    protected List<AuthorisationSupplier> parseScope(String claim) throws AuthenticationException {
        List<AuthorisationSupplier> authz = Splitter.on(' ').trimResults().splitToList(claim).stream()
                .filter(ScitokensScope::isSciTokenScope)
                .map(ScitokensScope::new)
                .collect(Collectors.toList());

        checkAuthentication(!authz.isEmpty(), "no authz statements in scitokens token");

        return authz;
    }
}
