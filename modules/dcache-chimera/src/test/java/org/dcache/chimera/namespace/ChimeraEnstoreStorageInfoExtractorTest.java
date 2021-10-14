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

import static com.google.common.base.Preconditions.checkState;
import static diskCacheV111.util.AccessLatency.ONLINE;
import static diskCacheV111.util.RetentionPolicy.REPLICA;
import static org.dcache.chimera.namespace.FsInodeBuilder.aFile;
import static org.dcache.chimera.namespace.InodeStorageInformationMatcher.matchesAnInodeStorageInformation;
import static org.dcache.util.StorageInfoBuilder.aStorageInfo;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.chimera.FsInode;
import org.junit.Test;

public class ChimeraEnstoreStorageInfoExtractorTest {

    private ChimeraEnstoreStorageInfoExtractor extractor;

    @Test
    public void shouldAcceptFlushUpdateWithLocation() throws Exception {
        given(anExtractor().withDefaultAccessLatency(ONLINE).withDefaultRetentionPolicy(REPLICA));
        FsInode inode = aFile().build();
        StorageInfo info = aStorageInfo()
              .withIsSetAddLocation()
              .withHsm("enstore-instance")
              .withKey("store", "store-group")
              .withKey("group", "store-subgroup")
              .withLocation("enstore://enstore-instance/specific-ID")
              .build();

        extractor.setStorageInfo(inode, info);

        verify(inode.getFs()).addInodeLocation(inode, 0, "enstore://enstore-instance/specific-ID");
        verify(inode.getFs()).setStorageInfo(eq(inode), argThat(matchesAnInodeStorageInformation()
              .withHsm("enstore-instance")
              .withInode(inode)
              .withStorageGroup("store-group")
              .withStorageSubgroup("store-subgroup")));
    }

    @Test
    public void shouldIgnoreFlushUpdateWithoutLocation() throws Exception {
        given(anExtractor().withDefaultAccessLatency(ONLINE).withDefaultRetentionPolicy(REPLICA));
        FsInode inode = aFile().build();
        StorageInfo info = aStorageInfo()
              .withIsSetAddLocation()
              .withHsm("enstore-instance")
              .withKey("store", "store-group")
              .withKey("group", "store-subgroup")
              // Note: NO locations.
              .build();

        extractor.setStorageInfo(inode, info);

        verify(inode.getFs(), never()).addInodeLocation(any(), anyInt(), any());
        verify(inode.getFs(), never()).setStorageInfo(any(), any());
    }

    private void given(ExtractorBuilder builder) {
        extractor = builder.build();
    }

    public ExtractorBuilder anExtractor() {
        return new ExtractorBuilder();
    }

    private static class ExtractorBuilder {

        private AccessLatency defaultAccessLatency;
        private RetentionPolicy defaultRetentionPolicy;

        public ExtractorBuilder withDefaultAccessLatency(AccessLatency latency) {
            defaultAccessLatency = latency;
            return this;
        }

        public ExtractorBuilder withDefaultRetentionPolicy(RetentionPolicy policy) {
            defaultRetentionPolicy = policy;
            return this;
        }

        public ChimeraEnstoreStorageInfoExtractor build() {
            checkState(defaultAccessLatency != null, "defaultAccessLatency not set");
            checkState(defaultRetentionPolicy != null, "defaultRetentionPolicy not set");
            return new ChimeraEnstoreStorageInfoExtractor(defaultAccessLatency,
                  defaultRetentionPolicy);
        }
    }
}
