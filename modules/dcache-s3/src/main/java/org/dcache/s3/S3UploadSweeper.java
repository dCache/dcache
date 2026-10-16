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
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.TYPE;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.namespace.FileType;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryStream;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes multipart uploads that were not completed or aborted within a maximum age.
 * <p>
 * Every hour, it sweeps the buckets that are directories directly below the door's root.
 * Buckets elsewhere, below users' own root directories, are swept when an upload is started in
 * them (see {@link DcacheBlobStore}).  Admin commands list, remove and sweep the uploads of any
 * bucket; they run in threads of their own, as directory listings are answered through the
 * cell's message queue, which a command would otherwise block.
 */
public class S3UploadSweeper implements CellCommandListener, CellInfoProvider, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3UploadSweeper.class);

    private static final long PERIOD = TimeUnit.HOURS.toMillis(1);

    private PnfsHandler pnfs;
    private DirectoryListSource lister;
    private FsPath root;
    private long maxAge;
    private TimeUnit maxAgeUnit;

    @GuardedBy("this")
    private Instant lastSweep;

    @GuardedBy("this")
    private int lastRemoved;

    /**
     * The door's PnfsHandler; the sweeper acts as root.
     */
    public void setPnfsHandler(PnfsHandler pnfs) {
        this.pnfs = new PnfsHandler(requireNonNull(pnfs), Subjects.ROOT, Restrictions.none());
    }

    public void setLister(DirectoryListSource lister) {
        this.lister = requireNonNull(lister);
    }

    /**
     * The door's root directory, whose subdirectories are swept.
     */
    public void setRoot(String root) {
        this.root = FsPath.create(root);
    }

    /**
     * How long an upload may take before it is removed.
     */
    public void setMaxAge(long maxAge) {
        this.maxAge = maxAge;
    }

    public void setMaxAgeUnit(TimeUnit unit) {
        maxAgeUnit = requireNonNull(unit);
    }

    public void setExecutor(ScheduledExecutorService executor) {
        executor.scheduleWithFixedDelay(this, PERIOD, PERIOD, TimeUnit.MILLISECONDS);
    }

    private S3MultipartUploads uploads() {
        return new S3MultipartUploads(pnfs, lister, Subjects.ROOT, Restrictions.none());
    }

    private Instant cutoff() {
        return Instant.now().minusMillis(maxAgeUnit.toMillis(maxAge));
    }

    @Override
    public void run() {
        try {
            int removed = sweepAll();
            synchronized (this) {
                lastSweep = Instant.now();
                lastRemoved = removed;
            }
        } catch (CacheException e) {
            LOGGER.warn("Failed to remove old multipart uploads: {}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            LOGGER.error("Bug while removing old multipart uploads", e);
        }
    }

    /**
     * Sweeps the buckets directly below the root.
     *
     * @return the number of uploads removed
     */
    int sweepAll() throws CacheException, InterruptedException {
        Instant cutoff = cutoff();
        var buckets = new ArrayList<FsPath>();
        try (DirectoryStream stream = lister.list(Subjects.ROOT, Restrictions.none(), root, null,
              Range.all(), Sets.immutableEnumSet(TYPE))) {
            for (DirectoryEntry entry : stream) {
                if (entry.getFileAttributes().getFileType() == FileType.DIR
                      && S3Paths.isBucketName(entry.getName())) {
                    buckets.add(root.child(entry.getName()));
                }
            }
        }
        S3MultipartUploads uploads = uploads();
        int removed = 0;
        for (FsPath bucket : buckets) {
            removed += uploads.sweep(bucket, cutoff);
        }
        return removed;
    }

    @Override
    public synchronized void getInfo(PrintWriter pw) {
        pw.println("Multipart upload max age : " + maxAge + " "
              + maxAgeUnit.name().toLowerCase());
        if (lastSweep != null) {
            pw.println("Last upload sweep        : " + lastSweep + ", removed " + lastRemoved);
        }
    }

    @Command(name = "s3 uploads ls", hint = "list multipart uploads of a bucket",
          description = "Lists the multipart uploads in progress in a bucket: upload id, start "
                + "time, number of parts, bytes uploaded and the key of the object.")
    public class ListCommand extends DelayedCommand<String> {

        @Argument(metaVar = "bucket", usage = "Path of the bucket's directory.")
        String bucket;

        @Override
        protected String execute() throws CacheException, InterruptedException {
            S3MultipartUploads uploads = uploads();
            var lines = new ArrayList<String>();
            for (var upload : uploads.list(FsPath.create(bucket))) {
                SortedMap<Integer, FileAttributes> parts =
                      uploads.parts(upload, Sets.immutableEnumSet(TYPE, SIZE));
                long size = parts.values().stream().mapToLong(FileAttributes::getSize).sum();
                lines.add(String.format("%s %s %5d %15d %s", upload.id(), upload.initiated(),
                      parts.size(), size, upload.key()));
            }
            return String.join("\n", lines);
        }
    }

    @Command(name = "s3 uploads rm", hint = "remove a multipart upload",
          description = "Removes a multipart upload and its parts, whatever its age.  The "
                + "client can no longer complete it.")
    public class RemoveCommand extends DelayedCommand<String> {

        @Argument(index = 0, metaVar = "bucket", usage = "Path of the bucket's directory.")
        String bucket;

        @Argument(index = 1, metaVar = "upload-id")
        String id;

        @Override
        protected String execute() throws CacheException, InterruptedException {
            S3MultipartUploads uploads = uploads();
            List<S3MultipartUploads.Upload> matching = uploads.list(FsPath.create(bucket))
                  .stream().filter(u -> u.id().equals(id)).toList();
            if (matching.isEmpty()) {
                throw new FileNotFoundCacheException("No such upload: " + id);
            }
            uploads.remove(matching.get(0).directory());
            return "Removed upload " + id + " of " + matching.get(0).key();
        }
    }

    @Command(name = "s3 uploads sweep", hint = "remove old multipart uploads",
          description = "Removes the multipart uploads older than the maximum age from a "
                + "bucket or, without argument, from all buckets directly below the door's "
                + "root, as is done every hour.")
    public class SweepCommand extends DelayedCommand<String> {

        @Argument(required = false, metaVar = "bucket", usage = "Path of the bucket's directory.")
        String bucket;

        @Override
        protected String execute() throws CacheException, InterruptedException {
            int removed = bucket == null
                  ? sweepAll()
                  : uploads().sweep(FsPath.create(bucket), cutoff());
            return "Removed " + removed + " uploads";
        }
    }
}
