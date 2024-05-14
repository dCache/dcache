/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 - 2020 Deutsches Elektronen-Synchrotron
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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import diskCacheV111.util.FsPath;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.stream.Stream;

public class DenyActivityRestrictionTests {

    @Test
    public void shouldRestrictAllActivity() {
        FsPath path = FsPath.create("/some/arbitrary/path");

        Restriction r = DenyActivityRestriction.restrictAllActivity();

        EnumSet.allOf(Activity.class).forEach(activity ->
              assertThat(r.isRestricted(activity, path), is(true)));
    }

    @Test
    public void shouldRestrictNoActivity() {
        FsPath path = FsPath.create("/some/arbitrary/path");

        Restriction r = DenyActivityRestriction.restrictNoActivity();

        EnumSet.allOf(Activity.class).forEach(activity ->
              assertThat(r.isRestricted(activity, path), is(false)));
    }

    @Test
    public void shouldRestrictSingleActivity() {
        FsPath path = FsPath.create("/some/arbitrary/path");

        for (Activity restricted : Activity.values()) {
            Restriction r = new DenyActivityRestriction(restricted);

            assertThat(r.isRestricted(restricted, path), is(true));

            Stream.of(Activity.values()).filter(activity -> !activity.equals(restricted))
                  .forEach(activity ->
                        assertThat(r.isRestricted(activity, path), is(false)));
        }
    }

    @Test
    public void shouldRestrictTwoActivities() {
        FsPath path = FsPath.create("/some/arbitrary/path");

        for (Activity ra1 : Activity.values()) {
            Stream.of(Activity.values()).filter(a -> !a.equals(ra1)).forEach(ra2 -> {
                Restriction r = new DenyActivityRestriction(ra1, ra2);
                EnumSet.allOf(Activity.class).forEach(activity ->
                      assertThat(r.isRestricted(activity, path),
                            is(Arrays.asList(ra1, ra2).contains(activity))));
            });
        }
    }

    @Test
    public void shouldHaveUnrestrictedChild() {
        FsPath path = FsPath.create("/some/arbitrary/path");

        for (Activity ra1 : Activity.values()) {
            Stream.of(Activity.values()).filter(a -> !a.equals(ra1)).forEach(ra2 -> {
                Restriction r = new DenyActivityRestriction(ra1, ra2);
                EnumSet.allOf(Activity.class).forEach(activity ->
                      assertThat(r.hasUnrestrictedChild(activity, path),
                            not(Arrays.asList(ra1, ra2).contains(activity))));
            });
        }
    }
}
