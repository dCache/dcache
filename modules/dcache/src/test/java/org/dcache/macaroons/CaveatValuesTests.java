/* dCache - http://www.dcache.org/
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
package org.dcache.macaroons;

import org.junit.Test;

import java.util.EnumSet;

import org.dcache.auth.attributes.Activity;

import static org.dcache.macaroons.CaveatValues.*;
import static org.dcache.macaroons.MacaroonContextBuilder.macaroonContext;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class CaveatValuesTests
{
    MacaroonContext _context;

    @Test
    public void shouldEncode()
    {
        given(macaroonContext().withUid(2).withGid(3,4).withUsername("paul"));

        String caveat = asIdentityCaveatValue(_context);

        assertThat(caveat, is(equalTo("2;3,4;paul")));
    }

    @Test
    public void shouldDecodeValidString() throws Exception
    {
        given(macaroonContext());

        parseIdentityCaveatValue(_context, "2;3,4;paul");

        assertThat(_context.getUid(), is(equalTo(2L)));
        assertThat(_context.getGids().length, is(equalTo(2)));
        assertThat(_context.getGids()[0], is(equalTo(3L)));
        assertThat(_context.getGids()[1], is(equalTo(4L)));
        assertThat(_context.getUsername(), is(equalTo("paul")));
    }

    @Test(expected=InvalidCaveatException.class)
    public void shouldRejectStringWithMissingSemicolon() throws Exception
    {
        given(macaroonContext());

        parseIdentityCaveatValue(_context, "2;3,paul");
    }

    @Test(expected=InvalidCaveatException.class)
    public void shouldRejectStringWithMissingGid() throws Exception
    {
        given(macaroonContext());

        parseIdentityCaveatValue(_context, "2;;paul");
    }

    @Test(expected=InvalidCaveatException.class)
    public void shouldRejectStringWithMissingUid() throws Exception
    {
        given(macaroonContext());

        parseIdentityCaveatValue(_context, ";3;paul");
    }

    @Test(expected=InvalidCaveatException.class)
    public void shouldRejectStringWithMissingUsername() throws Exception
    {
        given(macaroonContext());

        parseIdentityCaveatValue(_context, "2;3;");
    }

    @Test(expected=InvalidCaveatException.class)
    public void shouldRejectStringWithBadUid() throws Exception
    {
        given(macaroonContext());

        parseIdentityCaveatValue(_context, "foo;3;paul");
    }

    @Test(expected=InvalidCaveatException.class)
    public void shouldRejectStringWithBadGid() throws Exception
    {
        given(macaroonContext());

        parseIdentityCaveatValue(_context, "2;foo;paul");
    }

    @Test
    public void shouldEncodeActivity() throws Exception
    {
        String encoded = asActivityCaveatValue(EnumSet.of(Activity.DELETE, Activity.DOWNLOAD));

        assertThat(encoded, anyOf(equalTo("DELETE,DOWNLOAD"), equalTo("DOWNLOAD,DELETE")));
    }

    @Test
    public void shouldDecodeValidActivity() throws Exception
    {
        EnumSet<Activity> activities = parseActivityCaveatValue("DELETE,DOWNLOAD");

        assertThat(activities, hasSize(2));
        assertThat(activities, containsInAnyOrder(Activity.DELETE, Activity.DOWNLOAD));
    }

    @Test(expected=InvalidCaveatException.class)
    public void shouldRejectInvalidActivity() throws Exception
    {
        parseActivityCaveatValue("FOO");
    }

    void given(MacaroonContextBuilder builder)
    {
        _context = builder.build();
    }
}
