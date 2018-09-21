/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth.attributes;

import org.junit.Test;

import diskCacheV111.util.FsPath;

import static org.dcache.auth.attributes.Activity.LIST;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class PrefixRestrictionTest
{
    @Test
    public void shouldNotHaveUnrestrictedChildFromRootForEmptyPrefix()
    {
        Restriction r = new PrefixRestriction();

        assertThat(r.hasUnrestrictedChild(LIST, FsPath.ROOT), is(equalTo(false)));
    }


    @Test
    public void shouldNotHaveUnrestrictedChildFromPathForEmptyPrefix()
    {
        Restriction r = new PrefixRestriction();

        assertThat(r.hasUnrestrictedChild(LIST, FsPath.create("/foo/bar")), is(equalTo(false)));
    }

    @Test
    public void shouldHaveUnrestrictedChildFromRootForSinglePrefix()
    {
        Restriction r = new PrefixRestriction(FsPath.create("/foo/bar"));

        assertThat(r.hasUnrestrictedChild(LIST, FsPath.ROOT), is(equalTo(true)));
    }

    @Test
    public void shouldHaveUnrestrictedChildFromParentForSinglePrefix()
    {
        Restriction r = new PrefixRestriction(FsPath.create("/foo/bar"));

        assertThat(r.hasUnrestrictedChild(LIST, FsPath.create("/foo")), is(equalTo(true)));
    }

    @Test
    public void shouldHaveUnrestrictedChildFromSameDirForSinglePrefix()
    {
        Restriction r = new PrefixRestriction(FsPath.create("/foo/bar"));

        assertThat(r.hasUnrestrictedChild(LIST, FsPath.create("/foo/bar")), is(equalTo(false)));
    }

    @Test
    public void shouldHaveUnrestrictedChildFromSiblingDirForSinglePrefix()
    {
        Restriction r = new PrefixRestriction(FsPath.create("/foo/bar"));

        assertThat(r.hasUnrestrictedChild(LIST, FsPath.create("/foo/baz")), is(equalTo(false)));
    }

    @Test
    public void shouldHaveUnrestrictedChildFromChildForSinglePrefix()
    {
        Restriction r = new PrefixRestriction(FsPath.create("/foo/bar"));

        assertThat(r.hasUnrestrictedChild(LIST, FsPath.create("/foo/bar/baz")), is(equalTo(false)));
    }
}
