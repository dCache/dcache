/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2001 - 2016 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.doors;

import org.junit.Test;

import diskCacheV111.doors.DCapDoorInterpreterV3.Version;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests for parts of DCapDoorInterpreter
 */
public class DCapDoorInterpreterV3Tests
{
    @Test
    public void versionShouldEqual()
    {
        Version v1_2 = new Version(1, 2);
        Version v1_1 = new Version(1, 1);
        Version v2_2 = new Version(2, 2);
        Version v1_1_1 = new Version(1,1,1,null);
        Version v1_1_1_1 = new Version(1,1,1,"1");

        assertThat(v1_1, is(equalTo(v1_1)));
        assertThat(v1_2, is(equalTo(v1_2)));
        assertThat(v2_2, is(equalTo(v2_2)));
        assertThat(v1_1_1, is(equalTo(v1_1_1)));
        assertThat(v1_1_1_1, is(equalTo(v1_1_1_1)));

        assertThat(new Version(1,1), is(equalTo(v1_1)));
        assertThat(new Version(1,2), is(equalTo(v1_2)));
        assertThat(new Version(2,2), is(equalTo(v2_2)));
        assertThat(new Version(1,1,1,null), is(equalTo(v1_1_1)));
        assertThat(new Version(1,1,1,"1"), is(equalTo(v1_1_1_1)));

        assertThat(v1_1, not(equalTo(v1_2)));
        assertThat(v1_2, not(equalTo(v1_1)));

        assertThat(v2_2, not(equalTo(v1_2)));
        assertThat(v1_2, not(equalTo(v2_2)));

        assertThat(v2_2, not(equalTo(v1_1)));
        assertThat(v1_1, not(equalTo(v2_2)));

        assertThat(v1_1, is(not(equalTo(v1_1_1))));
        assertThat(v1_1_1, is(not(equalTo(v1_1))));

        assertThat(v1_1, is(not(equalTo(v1_1_1_1))));
        assertThat(v1_1_1_1, is(not(equalTo(v1_1))));

        assertThat(v1_1_1, is(not(equalTo(v1_1_1_1))));
        assertThat(v1_1_1_1, is(not(equalTo(v1_1_1))));
    }

    @Test
    public void versionShouldConstructFromString()
    {
        assertThat(new Version("1.2"), is(equalTo(new Version(1,2))));
        assertThat(new Version("1.2.3"), is(equalTo(new Version(1,2,3,null))));
        assertThat(new Version("1.2.3-4"), is(equalTo(new Version(1,2,3,"4"))));
    }

    @Test
    public void versionShouldMatchTo()
    {
        Version v1_2 = new Version(1, 2);
        Version v1_1 = new Version(1, 1);
        Version v2_1 = new Version(2, 1);
        Version v1_1_1 = new Version(1, 1, 1, null);
        Version v1_1_2 = new Version(1, 1, 2, null);
        Version v1_1_1_1 = new Version(1, 1, 1, "1");
        Version v1_1_1_2 = new Version(1, 1, 1, "2");

        assertThat(v1_1.matches(v1_1), is(equalTo(0)));
        assertThat(v1_2.matches(v1_2), is(equalTo(0)));
        assertThat(v2_1.matches(v2_1), is(equalTo(0)));
        assertThat(v1_1_1.matches(v1_1_1), is(equalTo(0)));
        assertThat(v1_1_2.matches(v1_1_2), is(equalTo(0)));
        assertThat(v1_1_1_1.matches(v1_1_1_1), is(equalTo(0)));
        assertThat(v1_1_1_2.matches(v1_1_1_2), is(equalTo(0)));
        assertThat(v1_1.matches(v1_1_1), is(equalTo(0)));
        assertThat(v1_1.matches(v1_1_1_1), is(equalTo(0)));
        assertThat(v1_1_1.matches(v1_1_1_1), is(equalTo(0)));

        assertThat(v1_1.matches(v1_2), is(equalTo(-1)));
        assertThat(v1_2.matches(v1_1), is(equalTo(1)));

        assertThat(v1_1.matches(v1_2), is(equalTo(-1)));
        assertThat(v2_1.matches(v1_1), is(equalTo(1)));

        assertThat(v1_2.matches(v2_1), is(equalTo(-1)));
        assertThat(v2_1.matches(v1_2), is(equalTo(1)));

        assertThat(v1_1_1.matches(v1_1), is(equalTo(1)));

        assertThat(v1_1_1.matches(v1_1_2), is(equalTo(-1)));
        assertThat(v1_1_2.matches(v1_1_1), is(equalTo(1)));

        assertThat(v1_1_1_1.matches(v1_1_1), is(equalTo(1)));

        assertThat(v1_1_1_1.matches(v1_1_1_2), is(equalTo(-1)));
        assertThat(v1_1_1_2.matches(v1_1_1_1), is(equalTo(1)));
    }
}
