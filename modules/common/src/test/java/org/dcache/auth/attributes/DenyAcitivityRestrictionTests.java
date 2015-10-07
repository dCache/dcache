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
package org.dcache.auth.attributes;

import org.junit.Test;

import diskCacheV111.util.FsPath;

import static org.dcache.auth.attributes.Activity.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DenyAcitivityRestrictionTests
{
    @Test
    public void shouldRestrictAllActivity()
    {
        FsPath path = new FsPath("/some/arbitrary/path");

        Restriction r = DenyActivityRestriction.restrictAllActivity();

        assertThat(r.isRestricted(DELETE, path), is(equalTo(true)));
        assertThat(r.isRestricted(DOWNLOAD, path), is(equalTo(true)));
        assertThat(r.isRestricted(LIST, path), is(equalTo(true)));
        assertThat(r.isRestricted(MANAGE, path), is(equalTo(true)));
        assertThat(r.isRestricted(READ_METADATA, path), is(equalTo(true)));
        assertThat(r.isRestricted(UPDATE_METADATA, path), is(equalTo(true)));
        assertThat(r.isRestricted(UPLOAD, path), is(equalTo(true)));
    }

    @Test
    public void shouldRestrictNoActivity()
    {
        FsPath path = new FsPath("/some/arbitrary/path");

        Restriction r = DenyActivityRestriction.restrictNoActivity();

        assertThat(r.isRestricted(DELETE, path), is(equalTo(false)));
        assertThat(r.isRestricted(DOWNLOAD, path), is(equalTo(false)));
        assertThat(r.isRestricted(LIST, path), is(equalTo(false)));
        assertThat(r.isRestricted(MANAGE, path), is(equalTo(false)));
        assertThat(r.isRestricted(READ_METADATA, path), is(equalTo(false)));
        assertThat(r.isRestricted(UPDATE_METADATA, path), is(equalTo(false)));
        assertThat(r.isRestricted(UPLOAD, path), is(equalTo(false)));
    }

    @Test
    public void shouldRestrictSingleActivity()
    {
        FsPath path = new FsPath("/some/arbitrary/path");

        Restriction r = new DenyActivityRestriction(DELETE);

        assertThat(r.isRestricted(DOWNLOAD, path), is(equalTo(false)));
        assertThat(r.isRestricted(LIST, path), is(equalTo(false)));
        assertThat(r.isRestricted(MANAGE, path), is(equalTo(false)));
        assertThat(r.isRestricted(READ_METADATA, path), is(equalTo(false)));
        assertThat(r.isRestricted(UPDATE_METADATA, path), is(equalTo(false)));
        assertThat(r.isRestricted(UPLOAD, path), is(equalTo(false)));

        assertThat(r.isRestricted(DELETE, path), is(equalTo(true)));
    }

    @Test
    public void shouldRestrictTwoActivities()
    {
        FsPath path = new FsPath("/some/arbitrary/path");

        Restriction r = new DenyActivityRestriction(DELETE, MANAGE);

        assertThat(r.isRestricted(DOWNLOAD, path), is(equalTo(false)));
        assertThat(r.isRestricted(LIST, path), is(equalTo(false)));
        assertThat(r.isRestricted(READ_METADATA, path), is(equalTo(false)));
        assertThat(r.isRestricted(UPDATE_METADATA, path), is(equalTo(false)));
        assertThat(r.isRestricted(UPLOAD, path), is(equalTo(false)));

        assertThat(r.isRestricted(DELETE, path), is(equalTo(true)));
        assertThat(r.isRestricted(MANAGE, path), is(equalTo(true)));
    }
}
