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

import static org.dcache.auth.attributes.Activity.DELETE;
import static org.dcache.auth.attributes.Activity.DOWNLOAD;
import static org.dcache.auth.attributes.Activity.LIST;
import static org.dcache.auth.attributes.Activity.MANAGE;
import static org.dcache.auth.attributes.Activity.READ_METADATA;
import static org.dcache.auth.attributes.Activity.UPDATE_METADATA;
import static org.dcache.auth.attributes.Activity.UPLOAD;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.theInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import diskCacheV111.util.FsPath;
import java.util.EnumSet;
import org.junit.Test;

public class RestrictionsTests {

    @Test
    public void shouldProvideRestrictionThatDeniesEverything() {
        Restriction restriction = Restrictions.denyAll();

        FsPath path = FsPath.create("/some/arbitrary/path");

        assertThat(restriction.isRestricted(DELETE, path), is(equalTo(true)));
        assertThat(restriction.isRestricted(DOWNLOAD, path), is(equalTo(true)));
        assertThat(restriction.isRestricted(LIST, path), is(equalTo(true)));
        assertThat(restriction.isRestricted(MANAGE, path), is(equalTo(true)));
        assertThat(restriction.isRestricted(READ_METADATA, path), is(equalTo(true)));
        assertThat(restriction.isRestricted(UPDATE_METADATA, path), is(equalTo(true)));
        assertThat(restriction.isRestricted(UPLOAD, path), is(equalTo(true)));
    }

    @Test
    public void shouldProvideRestrictionThatDeniesNothing() {
        Restriction restriction = Restrictions.none();

        FsPath path = FsPath.create("/some/arbitrary/path");

        assertThat(restriction.isRestricted(DELETE, path), is(equalTo(false)));
        assertThat(restriction.isRestricted(DOWNLOAD, path), is(equalTo(false)));
        assertThat(restriction.isRestricted(LIST, path), is(equalTo(false)));
        assertThat(restriction.isRestricted(MANAGE, path), is(equalTo(false)));
        assertThat(restriction.isRestricted(READ_METADATA, path), is(equalTo(false)));
        assertThat(restriction.isRestricted(UPDATE_METADATA, path), is(equalTo(false)));
        assertThat(restriction.isRestricted(UPLOAD, path), is(equalTo(false)));
    }

    @Test
    public void shouldProvideRestrictionThatDeniesModifying() {
        Restriction restriction = Restrictions.readOnly();

        FsPath path = FsPath.create("/some/arbitrary/path");

        assertThat(restriction.isRestricted(DOWNLOAD, path), is(equalTo(false)));
        assertThat(restriction.isRestricted(LIST, path), is(equalTo(false)));
        assertThat(restriction.isRestricted(READ_METADATA, path), is(equalTo(false)));

        assertThat(restriction.isRestricted(DELETE, path), is(equalTo(true)));
        assertThat(restriction.isRestricted(MANAGE, path), is(equalTo(true)));
        assertThat(restriction.isRestricted(UPLOAD, path), is(equalTo(true)));
        assertThat(restriction.isRestricted(UPDATE_METADATA, path), is(equalTo(true)));
    }

    @Test
    public void shouldConcatNoneAsUnrestricted() {
        Restriction concat = Restrictions.concat();

        assertThat(concat, is(equalTo(Restrictions.none())));
    }

    @Test
    public void shouldConcatNoneAsSame() {
        Restriction none = Restrictions.none();

        Restriction concat = Restrictions.concat(none);

        assertThat(concat, is(theInstance(none)));
    }

    @Test
    public void shouldConcatDenyAllAsSame() {
        Restriction denyAll = Restrictions.denyAll();

        Restriction concat = Restrictions.concat(denyAll);

        assertThat(concat, is(theInstance(denyAll)));
    }

    @Test
    public void shouldConcatReadOnlyAllAsSame() {
        Restriction readOnly = Restrictions.readOnly();

        Restriction concat = Restrictions.concat(readOnly);

        assertThat(concat, is(theInstance(readOnly)));
    }

    @Test
    public void shouldConcatNoneNoneAsNone() {
        Restriction none = Restrictions.none();

        Restriction concat = Restrictions.concat(none, none);

        assertThat(concat, is(equalTo(none)));
    }

    @Test
    public void shouldConcatNoneReadOnlyAsReadOnly() {
        Restriction none = Restrictions.none();
        Restriction readOnly = Restrictions.readOnly();

        Restriction concat = Restrictions.concat(none, readOnly);

        assertThat(concat, is(equalTo(readOnly)));
    }

    @Test
    public void shouldConcatReadOnlyNoneAsReadOnly() {
        Restriction readOnly = Restrictions.readOnly();
        Restriction none = Restrictions.none();

        Restriction concat = Restrictions.concat(readOnly, none);

        assertThat(concat, is(equalTo(readOnly)));
    }

    @Test
    public void shouldConcatReadOnlyReadOnlyAsReadOnly() {
        Restriction readOnly = Restrictions.readOnly();

        Restriction concat = Restrictions.concat(readOnly, readOnly);

        assertThat(concat, is(equalTo(readOnly)));
    }

    @Test
    public void shouldConcatReadOnlyDenyAllAsDenyAll() {
        Restriction readOnly = Restrictions.readOnly();
        Restriction denyAll = Restrictions.denyAll();

        Restriction concat = Restrictions.concat(readOnly, denyAll);

        assertThat(concat, is(equalTo(denyAll)));
    }

    @Test
    public void shouldConcatDenyAllReadOnlyAsDenyAll() {
        Restriction denyAll = Restrictions.denyAll();
        Restriction readOnly = Restrictions.readOnly();

        Restriction concat = Restrictions.concat(denyAll, readOnly);

        assertThat(concat, is(equalTo(denyAll)));
    }

    @Test
    public void shouldConcatNoneDenyAllAsDenyAll() {
        Restriction none = Restrictions.none();
        Restriction denyAll = Restrictions.denyAll();

        Restriction concat = Restrictions.concat(none, denyAll);

        assertThat(concat, is(equalTo(denyAll)));
    }

    @Test
    public void shouldConcatDenyAllNoneAsDenyAll() {
        Restriction denyAll = Restrictions.denyAll();
        Restriction none = Restrictions.none();

        Restriction concat = Restrictions.concat(denyAll, none);

        assertThat(concat, is(equalTo(denyAll)));
    }

    @Test
    public void shouldConcatDenyAllDenyAllAsDenyAll() {
        Restriction denyAll = Restrictions.denyAll();

        Restriction concat = Restrictions.concat(denyAll, denyAll);

        assertThat(concat, is(equalTo(denyAll)));
    }

    @Test
    public void shouldConcatTwoNonSubsumptionAsComposite() {
        Restriction denyDownload = new DenyActivityRestriction(DOWNLOAD);
        Restriction denyUpload = new DenyActivityRestriction(UPLOAD);

        Restriction concat = Restrictions.concat(denyUpload, denyDownload);

        assertThat(concat, is(not(equalTo(denyDownload))));
        assertThat(concat, is(not(equalTo(denyUpload))));

        FsPath path = FsPath.create("/some/arbitrary/path");

        assertThat(concat.isRestricted(DOWNLOAD, path), is(equalTo(true)));
        assertThat(concat.isRestricted(UPLOAD, path), is(equalTo(true)));

        assertThat(concat.isRestricted(DELETE, path), is(equalTo(false)));
        assertThat(concat.isRestricted(LIST, path), is(equalTo(false)));
        assertThat(concat.isRestricted(MANAGE, path), is(equalTo(false)));
        assertThat(concat.isRestricted(READ_METADATA, path), is(equalTo(false)));
        assertThat(concat.isRestricted(UPDATE_METADATA, path), is(equalTo(false)));
    }


    @Test
    public void shouldConcatTwoNonSubsumptionAndSubsumptionAsComposite() {
        Restriction denyDownload = new DenyActivityRestriction(DOWNLOAD);
        Restriction denyUpload = new DenyActivityRestriction(UPLOAD);
        Restriction denyDownloadAndDelete = new DenyActivityRestriction(DOWNLOAD, DELETE);

        Restriction concat = Restrictions.concat(denyUpload, denyDownload, denyDownloadAndDelete);

        assertThat(concat, is(not(equalTo(denyDownload))));
        assertThat(concat, is(not(equalTo(denyUpload))));

        FsPath path = FsPath.create("/some/arbitrary/path");

        assertThat(concat.isRestricted(DOWNLOAD, path), is(equalTo(true)));
        assertThat(concat.isRestricted(UPLOAD, path), is(equalTo(true)));
        assertThat(concat.isRestricted(DELETE, path), is(equalTo(true)));

        assertThat(concat.isRestricted(LIST, path), is(equalTo(false)));
        assertThat(concat.isRestricted(MANAGE, path), is(equalTo(false)));
        assertThat(concat.isRestricted(READ_METADATA, path), is(equalTo(false)));
        assertThat(concat.isRestricted(UPDATE_METADATA, path), is(equalTo(false)));
    }

    @Test
    public void shouldCombinePathAndActivityRestrictions() {
        Restriction onlyUpload = new DenyActivityRestriction(
              EnumSet.complementOf(EnumSet.of(UPLOAD)));
        Restriction pathRestriction = new PrefixRestriction(
              FsPath.create("/foo/bar/latest-results.dat"));

        Restriction concat = Restrictions.concat(onlyUpload, pathRestriction);

        assertThat(concat.isRestricted(UPLOAD, FsPath.ROOT), is(equalTo(true)));
        assertThat(concat.isRestricted(DOWNLOAD, FsPath.ROOT), is(equalTo(true)));
        assertThat(concat.hasUnrestrictedChild(UPLOAD, FsPath.ROOT), is(equalTo(true)));
        assertThat(concat.hasUnrestrictedChild(DOWNLOAD, FsPath.ROOT), is(equalTo(false)));

        assertThat(concat.isRestricted(UPLOAD, FsPath.create("/foo")), is(equalTo(true)));
        assertThat(concat.isRestricted(DOWNLOAD, FsPath.create("/foo")), is(equalTo(true)));
        assertThat(concat.hasUnrestrictedChild(UPLOAD, FsPath.create("/foo")), is(equalTo(true)));
        assertThat(concat.hasUnrestrictedChild(DOWNLOAD, FsPath.create("/foo")),
              is(equalTo(false)));

        assertThat(concat.isRestricted(UPLOAD, FsPath.create("/foo/bar")), is(equalTo(true)));
        assertThat(concat.isRestricted(DOWNLOAD, FsPath.create("/foo/bar")), is(equalTo(true)));
        assertThat(concat.hasUnrestrictedChild(UPLOAD, FsPath.create("/foo/bar")),
              is(equalTo(true)));
        assertThat(concat.hasUnrestrictedChild(DOWNLOAD, FsPath.create("/foo/bar")),
              is(equalTo(false)));

        assertThat(concat.isRestricted(UPLOAD, FsPath.create("/foo/bar/latest-results.dat")),
              is(equalTo(false)));
        assertThat(concat.isRestricted(DOWNLOAD, FsPath.create("/foo/bar/latest-results.dat")),
              is(equalTo(true)));
        assertThat(
              concat.hasUnrestrictedChild(UPLOAD, FsPath.create("/foo/bar/latest-results.dat")),
              is(equalTo(false)));
        assertThat(
              concat.hasUnrestrictedChild(DOWNLOAD, FsPath.create("/foo/bar/latest-results.dat")),
              is(equalTo(false)));

        assertThat(concat.isRestricted(UPLOAD, FsPath.create("/baz")), is(equalTo(true)));
        assertThat(concat.isRestricted(DOWNLOAD, FsPath.create("/baz")), is(equalTo(true)));
        assertThat(concat.hasUnrestrictedChild(UPLOAD, FsPath.create("/baz")), is(equalTo(false)));
        assertThat(concat.hasUnrestrictedChild(DOWNLOAD, FsPath.create("/baz")),
              is(equalTo(false)));

        assertThat(concat.isRestricted(UPLOAD, FsPath.create("/foo/baz")), is(equalTo(true)));
        assertThat(concat.isRestricted(DOWNLOAD, FsPath.create("/foo/baz")), is(equalTo(true)));
        assertThat(concat.hasUnrestrictedChild(UPLOAD, FsPath.create("/foo/baz")),
              is(equalTo(false)));
        assertThat(concat.hasUnrestrictedChild(DOWNLOAD, FsPath.create("/foo/baz")),
              is(equalTo(false)));
    }
}
