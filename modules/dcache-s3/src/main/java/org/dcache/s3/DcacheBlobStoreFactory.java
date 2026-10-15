/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.s3;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.PnfsHandler;
import java.util.function.Supplier;
import org.dcache.util.list.DirectoryListSource;
import org.gaul.s3proxy.blobstore.BlobStore;

/**
 * Creates a {@link DcacheBlobStore} for each S3 request.
 */
public class DcacheBlobStoreFactory implements S3BlobStoreFactory {

    private final PnfsHandler pnfs;
    private final DirectoryListSource lister;
    private final S3ObjectReader reader;
    private final S3ObjectRestorer restorer;
    private final int maxListEntries;

    /**
     * @param maxListEntries how many directory entries a listing without delimiter reads before
     *                       it ends the page early
     */
    public DcacheBlobStoreFactory(PnfsHandler pnfs, DirectoryListSource lister,
          S3ObjectReader reader, S3ObjectRestorer restorer, int maxListEntries) {
        this.pnfs = requireNonNull(pnfs);
        this.lister = requireNonNull(lister);
        this.reader = requireNonNull(reader);
        this.restorer = requireNonNull(restorer);
        this.maxListEntries = maxListEntries;
    }

    @Override
    public BlobStore create(Supplier<S3User> user) {
        return new DcacheBlobStore(user, pnfs, lister, reader, restorer, maxListEntries);
    }
}
