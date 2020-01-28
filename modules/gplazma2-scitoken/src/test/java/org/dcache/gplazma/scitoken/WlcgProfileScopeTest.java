/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.scitoken;

import org.junit.Test;

import java.util.Optional;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;

import static org.dcache.auth.attributes.Activity.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class WlcgProfileScopeTest
{
    @Test
    public void shouldIdentifyStorageReadScope()
    {
        assertTrue(WlcgProfileScope.isWlcgProfileScope("storage.read:/"));
    }

    @Test
    public void shouldIdentifyStorageCreateScope()
    {
        assertTrue(WlcgProfileScope.isWlcgProfileScope("storage.create:/"));
    }

    @Test
    public void shouldIdentifyStorageModifyScope()
    {
        assertTrue(WlcgProfileScope.isWlcgProfileScope("storage.modify:/"));
    }

    @Test
    public void shouldNotIdentifyStorageWriteScope()
    {
        assertFalse(WlcgProfileScope.isWlcgProfileScope("storage.write:/"));
    }

    @Test
    public void shouldParseReadScope()
    {
        WlcgProfileScope scope = new WlcgProfileScope("storage.read:/");

        Optional<Authorisation> maybeAuth = scope.authorisation(FsPath.create("/VOs/wlcg"));

        assertTrue(maybeAuth.isPresent());

        Authorisation auth = maybeAuth.get();

        assertThat(auth.getPath(), equalTo(FsPath.create("/VOs/wlcg")));
        assertThat(auth.getActivity(), containsInAnyOrder(LIST, READ_METADATA, DOWNLOAD));
    }
}
