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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.Optional;
import java.util.OptionalLong;

import diskCacheV111.util.FsPath;

import static org.dcache.auth.attributes.Activity.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.dcache.macaroons.MacaroonContextBuilder.macaroonContext;


public class MacaroonContextTests
{
    MacaroonContext _context;

    @Test
    public void shouldAcceptAbsoluteRootPath() throws Exception
    {
        given(macaroonContext().withRoot("/path"));

        _context.updateRoot("/other");

        assertThat(_context.getRoot(), is(equalTo(Optional.of(FsPath.create("/path/other")))));
    }

    @Test
    public void shouldAcceptRelativeRootPath() throws Exception
    {
        given(macaroonContext().withRoot("/path"));

        _context.updateRoot("other");

        assertThat(_context.getRoot(), is(equalTo(Optional.of(FsPath.create("/path/other")))));
    }

    @Test
    public void shouldNotWalkOutsideOfRoot() throws Exception
    {
        given(macaroonContext().withRoot("/path"));

        _context.updateRoot("..");

        assertThat(_context.getRoot(), is(equalTo(Optional.of(FsPath.create("/path")))));
    }

    @Test(expected=InvalidCaveatException.class)
    public void shouldRejectRootOutsideOfPath() throws Exception
    {
        given(macaroonContext().withPath("/path"));

        _context.updateRoot("/other");
    }

    @Test
    public void shouldAcceptRootInsideOfPath() throws Exception
    {
        given(macaroonContext().withHome("/path/subdir/home").withPath("/path/subdir"));

        _context.updateRoot("/path");

        assertThat(_context.getRoot(), is(equalTo(Optional.of(FsPath.create("/path")))));
        assertThat(_context.getPath(), is(equalTo(Optional.of(FsPath.create("/subdir")))));
        assertThat(_context.getHome(), is(equalTo(Optional.of(FsPath.create("/subdir/home")))));
    }

    @Test
    public void shouldAcceptNewHome() throws Exception
    {
        given(macaroonContext().withHome("/users/paul"));

        _context.updateHome("/data/paul");

        assertThat(_context.getHome(), is(equalTo(Optional.of(FsPath.create("/data/paul")))));
    }

    @Test
    public void shouldAcceptAbsolutePath() throws Exception
    {
        given(macaroonContext().withPath("/users/paul"));

        _context.updatePath("/dir");

        assertThat(_context.getPath(), is(equalTo(Optional.of(FsPath.create("/users/paul/dir")))));
    }

    @Test
    public void shouldAcceptRelativePath() throws Exception
    {
        given(macaroonContext().withPath("/users/paul"));

        _context.updatePath("dir");

        assertThat(_context.getPath(), is(equalTo(Optional.of(FsPath.create("/users/paul/dir")))));
    }

    @Test
    public void shouldResetHomeWhenNewRootMakesOldUnreachable() throws Exception
    {
        given(macaroonContext().withHome("/users/paul"));

        _context.updateRoot("/data");

        assertThat(_context.getHome(), is(equalTo(Optional.empty())));
    }

    @Test
    public void shouldRestrictToDownloadAndReaddMetadataUnrestrictedActivities() throws Exception
    {
        given(macaroonContext().withHome("/users/paul"));

        _context.updateAllowedActivities(EnumSet.of(DOWNLOAD));

        assertThat(_context.getAllowedActivities(), is(equalTo(Optional.of(EnumSet.of(DOWNLOAD,READ_METADATA)))));
    }

    @Test
    public void shouldRestrictToDownloadListReaddmetadataUnrestrictedActivities() throws Exception
    {
        given(macaroonContext().withHome("/users/paul"));

        _context.updateAllowedActivities(EnumSet.of(DOWNLOAD, LIST));

        assertThat(_context.getAllowedActivities(), is(equalTo(Optional.of(EnumSet.of(DOWNLOAD,LIST,READ_METADATA)))));
    }

    @Test
    public void shouldRestrictToListReadmetadataRestrictedActivities() throws Exception
    {
        given(macaroonContext().withHome("/users/paul").withAllowedActivities(DOWNLOAD, LIST));

        _context.updateAllowedActivities(EnumSet.of(LIST));

        assertThat(_context.getAllowedActivities(), is(equalTo(Optional.of(EnumSet.of(LIST,READ_METADATA)))));
    }

    @Test(expected=InvalidCaveatException.class)
    public void shouldRejectAdditionalActivities() throws Exception
    {
        given(macaroonContext().withHome("/users/paul").withAllowedActivities(DOWNLOAD, LIST));

        _context.updateAllowedActivities(EnumSet.of(DOWNLOAD,UPLOAD,LIST));
    }

    @Test
    public void shouldHaveInitiallyNoExpiry() throws Exception
    {
        given(macaroonContext());

        assertThat(_context.getExpiry(), is(equalTo(Optional.empty())));
    }

    @Test
    public void shouldAcceptExpiry() throws Exception
    {
        Instant expiry = Instant.now().plus(5, ChronoUnit.MINUTES);
        given(macaroonContext());

        _context.updateExpiry(expiry);

        assertThat(_context.getExpiry(), is(equalTo(Optional.of(expiry))));
    }

    @Test
    public void shouldAcceptMoreRecentExpiry() throws Exception
    {
        Instant earlierExpiry = Instant.now().plus(5, ChronoUnit.MINUTES);
        Instant laterExpiry = earlierExpiry.plus(2, ChronoUnit.MINUTES);
        given(macaroonContext().withExpiry(laterExpiry));

        _context.updateExpiry(earlierExpiry);

        assertThat(_context.getExpiry(), is(equalTo(Optional.of(earlierExpiry))));
    }


    @Test
    public void shouldIgnoreLessRecentExpiry() throws Exception
    {
        Instant earlierExpiry = Instant.now().plus(5, ChronoUnit.MINUTES);
        Instant laterExpiry = earlierExpiry.plus(2, ChronoUnit.MINUTES);
        given(macaroonContext().withExpiry(earlierExpiry));

        _context.updateExpiry(laterExpiry);

        assertThat(_context.getExpiry(), is(equalTo(Optional.of(earlierExpiry))));
    }

    @Test
    public void shouldHaveInitiallyNoUploadLimit() throws Exception
    {
        given(macaroonContext());

        assertThat(_context.getMaxUpload(), is(equalTo(OptionalLong.empty())));
    }

    @Test
    public void shouldAcceptUploadLimit() throws Exception
    {
        given(macaroonContext().withMaxUpload(1024));

        assertThat(_context.getMaxUpload(), is(equalTo(OptionalLong.of(1024))));
    }

    @Test
    public void shouldNotUpdateMaxUploadLimitWithLargerValue() throws Exception
    {
        given(macaroonContext().withMaxUpload(1024).withMaxUpload(2048));

        assertThat(_context.getMaxUpload(), is(equalTo(OptionalLong.of(1024))));
    }

    @Test
    public void shouldUpdateMaxUploadLimitWithSmallerValue() throws Exception
    {
        given(macaroonContext().withMaxUpload(2048).withMaxUpload(1024));

        assertThat(_context.getMaxUpload(), is(equalTo(OptionalLong.of(1024))));
    }

    void given(MacaroonContextBuilder builder)
    {
        _context = builder.build();
    }
}
