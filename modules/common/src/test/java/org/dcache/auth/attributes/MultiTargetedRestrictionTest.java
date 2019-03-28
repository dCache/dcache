/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;

import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultiTargetedRestrictionTest
{
    private static final FsPath TARGET = FsPath.create("/path/to/dir");

    @Test
    public void shouldBeEqualIffHasSameEffect()
    {
        List<Authorisation> authorisations1 = new ArrayList<>();
        authorisations1.add(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET));
        authorisations1.add(new Authorisation(EnumSet.of(Activity.UPLOAD), FsPath.create("/completely/different/path")));
        Restriction restriction1 = new MultiTargetedRestriction(authorisations1);

        List<Authorisation> authorisations2 = new ArrayList<>();
        authorisations2.add(new Authorisation(EnumSet.of(Activity.UPLOAD), FsPath.create("/completely/different/path")));
        authorisations2.add(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET));
        Restriction restriction2 = new MultiTargetedRestriction(authorisations2);

        Restriction restriction3 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET)));

        assertThat(restriction1.equals(restriction2), is(true));
        assertThat(restriction2.equals(restriction1), is(true));
        assertThat(restriction1.equals(restriction3), is(false));
        assertThat(restriction3.equals(restriction1), is(false));
        assertThat(restriction2.equals(restriction3), is(false));
        assertThat(restriction3.equals(restriction2), is(false));
    }

    @Test
    public void shouldForbidEverythingWithNoAllowPath()
    {
        Restriction restriction = new MultiTargetedRestriction(Collections.emptyList());

        assertThat(restriction.isRestricted(Activity.LIST, FsPath.ROOT), is(true));
        assertThat(restriction.isRestricted(Activity.LIST, TARGET), is(true));
    }

    @Test
    public void shouldAllowActivityInPath()
    {
        Restriction restriction = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET)));

        assertThat(restriction.isRestricted(Activity.DOWNLOAD, TARGET), is(false));
        assertThat(restriction.isRestricted(Activity.DOWNLOAD, TARGET.child("file")), is(false));
    }

    @Test
    public void shouldForbidActivityOnParent()
    {
        Restriction restriction = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET)));

        assertThat(restriction.isRestricted(Activity.DOWNLOAD, TARGET.parent()), is(true));
        assertThat(restriction.isRestricted(Activity.DOWNLOAD, FsPath.ROOT), is(true));
    }

    @Test
    public void shouldAllowListOnParent()
    {
        Restriction restriction = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET)));

        assertThat(restriction.isRestricted(Activity.LIST, TARGET.parent()), is(false));
        assertThat(restriction.isRestricted(Activity.LIST, FsPath.ROOT), is(false));
    }

    @Test
    public void shouldSubsumeIfEqual()
    {
        Restriction restriction1 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET)));
        Restriction restriction2 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET)));

        assertThat(restriction1.isSubsumedBy(restriction2), is(true));
    }

    @Test
    public void shouldSubsumeIfPathDecendent()
    {
        FsPath decendent = TARGET.child("subdir");
        Restriction restriction1 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET)));
        Restriction restriction2 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), decendent)));

        assertThat(restriction1.isSubsumedBy(restriction2), is(true));
        assertThat(restriction2.isSubsumedBy(restriction1), is(false));
    }

    @Test
    public void shouldSubsumeIfMoreRestrictive()
    {
        Restriction restriction1 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD, Activity.LIST), TARGET)));
        Restriction restriction2 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET)));

        assertThat(restriction1.isSubsumedBy(restriction2), is(true));
        assertThat(restriction2.isSubsumedBy(restriction1), is(false));
    }

    @Test
    public void shouldNotSubsumeIfPathsNotParentChild()
    {
        Restriction restriction1 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET.child("foo"))));
        Restriction restriction2 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET.child("bar"))));

        assertThat(restriction1.isSubsumedBy(restriction2), is(false));
        assertThat(restriction2.isSubsumedBy(restriction1), is(false));
    }

    @Test
    public void shouldNotSubsumeOnActivity()
    {
        Restriction restriction1 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD, Activity.LIST), TARGET)));
        Restriction restriction2 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD, Activity.UPLOAD), TARGET)));

        assertThat(restriction1.isSubsumedBy(restriction2), is(false));
        assertThat(restriction2.isSubsumedBy(restriction1), is(false));
    }

    @Test
    public void shouldSubsumeOnMultiplePaths()
    {
        List<Authorisation> authorisations = new ArrayList<>();
        authorisations.add(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET));
        authorisations.add(new Authorisation(EnumSet.of(Activity.UPLOAD), FsPath.create("/completely/different/path")));
        Restriction restriction1 = new MultiTargetedRestriction(authorisations);
        Restriction restriction2 = new MultiTargetedRestriction(singleton(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET)));

        assertThat(restriction1.isSubsumedBy(restriction2), is(true));
        assertThat(restriction2.isSubsumedBy(restriction1), is(false));
    }

    @Test
    public void shouldIdentifyUnrestrictedChild()
    {
        List<Authorisation> authorisations = new ArrayList<>();
        authorisations.add(new Authorisation(EnumSet.of(Activity.DOWNLOAD), TARGET));
        authorisations.add(new Authorisation(EnumSet.of(Activity.UPLOAD), FsPath.create("/completely/different/path")));
        Restriction restriction = new MultiTargetedRestriction(authorisations);

        assertThat(restriction.hasUnrestrictedChild(Activity.DOWNLOAD, TARGET), is(true));
        assertThat(restriction.hasUnrestrictedChild(Activity.DOWNLOAD, TARGET.parent()), is(true));
        assertThat(restriction.hasUnrestrictedChild(Activity.DOWNLOAD, TARGET.child("some-child")), is(true));
        assertThat(restriction.hasUnrestrictedChild(Activity.DOWNLOAD, TARGET.parent().child("some-sibling")), is(false));

        assertThat(restriction.hasUnrestrictedChild(Activity.UPLOAD, TARGET), is(false));
    }
}
