/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.chimera.namespace;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.FsInode;

import static com.google.common.base.Preconditions.checkState;
import static diskCacheV111.util.RetentionPolicy.REPLICA;
import static diskCacheV111.util.AccessLatency.ONLINE;
import static org.dcache.chimera.namespace.FsInodeBuilder.aFile;
import static org.dcache.chimera.namespace.InodeStorageInformationMatcher.matchesAnInodeStorageInformation;
import static org.dcache.util.StorageInfoBuilder.aStorageInfo;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ChimeraOsmStorageInfoExtractorTest
{
    private ChimeraOsmStorageInfoExtractor extractor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldAcceptFlushUpdateWithLocation() throws Exception
    {
        given(anExtractor().withDefaultAccessLatency(ONLINE).withDefaultRetentionPolicy(REPLICA));
        FsInode inode = aFile().build();
        StorageInfo info = aStorageInfo()
                        .withIsSetAddLocation()
                        .withHsm("osm-instance")
                        .withKey("store", "store-group")
                        .withKey("group", "store-subgroup")
                        .withLocation("osm://osm-instance/specific-ID")
                        .build();

        extractor.setStorageInfo(inode, info);

        verify(inode.getFs()).addInodeLocation(inode, 0, "osm://osm-instance/specific-ID");
        verify(inode.getFs()).setStorageInfo(eq(inode), argThat(matchesAnInodeStorageInformation()
                        .withHsm("osm-instance")
                        .withInode(inode)
                        .withStorageGroup("store-group")
                        .withStorageSubgroup("store-subgroup")));
    }

    @Test
    public void shouldIngoreNonFlushUpdate() throws Exception
    {
        given(anExtractor().withDefaultAccessLatency(ONLINE).withDefaultRetentionPolicy(REPLICA));
        FsInode inode = aFile().build();
        StorageInfo info = aStorageInfo()
                        // Note: withIsSetAddLocation NOT called.
                        .withHsm("osm-instance")
                        .withKey("store", "store-group")
                        .withKey("group", "store-subgroup")
                        .withLocation("osm://osm-instance/specific-ID")
                        .build();

        extractor.setStorageInfo(inode, info);

        verify(inode.getFs(), never()).addInodeLocation(any(), anyInt(), any());
        verify(inode.getFs(), never()).setStorageInfo(any(), any());
    }

    @Test
    public void shouldRejectFlushUpdateWithNoLocation() throws Exception
    {
        thrown.expect(aCacheExceptionWithRc(10031));

        given(anExtractor().withDefaultAccessLatency(ONLINE).withDefaultRetentionPolicy(REPLICA));

        extractor.setStorageInfo(aFile().build(),
                aStorageInfo()
                        .withIsSetAddLocation()
                        .withHsm("osm-instance")
                        .withKey("store", "store-group")
                        .withKey("group", "store-subgroup")
                        // Note: there are no locations defined.
                        .build());
    }

    private void given(ExtractorBuilder builder)
    {
        extractor = builder.build();
    }

    public ExtractorBuilder anExtractor()
    {
        return new ExtractorBuilder();
    }

    private static class ExtractorBuilder
    {
        private AccessLatency defaultAccessLatency;
        private RetentionPolicy defaultRetentionPolicy;

        public ExtractorBuilder withDefaultAccessLatency(AccessLatency latency)
        {
            defaultAccessLatency = latency;
            return this;
        }

        public ExtractorBuilder withDefaultRetentionPolicy(RetentionPolicy policy)
        {
            defaultRetentionPolicy = policy;
            return this;
        }

        public ChimeraOsmStorageInfoExtractor build()
        {
            checkState(defaultAccessLatency != null, "defaultAccessLatency not set");
            checkState(defaultRetentionPolicy != null, "defaultRetentionPolicy not set");
            return new ChimeraOsmStorageInfoExtractor(defaultAccessLatency,
                    defaultRetentionPolicy);
        }
    }

    private static CacheExceptionMatcher aCacheExceptionWithRc(int rc)
    {
        return new CacheExceptionMatcher(rc);
    }

    public static class CacheExceptionMatcher extends BaseMatcher<Object>
    {
        private final int expectedRc;

        public CacheExceptionMatcher(int expectedRc)
        {
            this.expectedRc = expectedRc;
        }

        @Override
        public boolean matches(Object actual)
        {
            return (actual instanceof CacheException)
                    &&
                    ((CacheException)actual).getRc() == expectedRc;
        }

        @Override
        public void describeMismatch(Object actual, Description mismatchDescription)
        {
            if (!(actual instanceof CacheException)) {
                mismatchDescription.appendText(actual.getClass().getCanonicalName());
            } else {
                CacheException other = (CacheException)actual;
                mismatchDescription.appendText("a CacheException with rc "
                        + other.getRc());
            }
        }

        @Override
        public void describeTo(Description description)
        {
            description.appendText("a CacheException with rc " + expectedRc);
        }
    }
}
