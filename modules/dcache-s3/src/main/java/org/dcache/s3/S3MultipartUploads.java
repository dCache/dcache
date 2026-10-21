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
import static org.dcache.namespace.FileAttribute.CREATION_TIME;
import static org.dcache.namespace.FileAttribute.MODIFICATION_TIME;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.namespace.FileAttribute.XATTR;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PnfsHandler;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryStream;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The multipart uploads in progress in a bucket, kept in the namespace.
 * <p>
 * Each upload is a directory {@code <bucket>/.s3-multipart/<upload id>} holding the uploaded
 * parts as files named by their part number.  The directory's extended attributes hold the key
 * of the object being uploaded ({@code s3.key}) and the object's metadata, as for objects.
 */
class S3MultipartUploads {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3MultipartUploads.class);

    /**
     * The extended attribute of an upload directory holding the object's key.
     */
    static final String XATTR_KEY = "s3.key";

    /**
     * The highest part number S3 allows.
     */
    static final int MAX_PART_NUMBER = 10_000;

    private static final Pattern UPLOAD_ID = Pattern.compile(
          "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

    private static final Pattern PART_NUMBER = Pattern.compile("[1-9][0-9]{0,4}");

    // Sent to PnfsManager, so they must be serializable.
    private static final Set<FileAttribute> UPLOAD_ATTRIBUTES =
          Sets.immutableEnumSet(TYPE, CREATION_TIME, XATTR);

    private static final Set<FileAttribute> ENTRY_ATTRIBUTES =
          Sets.immutableEnumSet(TYPE, MODIFICATION_TIME);

    /**
     * An upload in progress.
     *
     * @param id        the upload id
     * @param key       the key of the object being uploaded
     * @param initiated when the upload was started
     * @param xattrs    the object's metadata, as extended attributes
     * @param directory the directory holding the parts
     */
    record Upload(String id, String key, Instant initiated, Map<String, String> xattrs,
                  FsPath directory) {
    }

    private final PnfsHandler pnfs;
    private final DirectoryListSource lister;
    private final Subject subject;
    private final Restriction restriction;

    /**
     * @param pnfs        acting as {@code subject} with {@code restriction}
     * @param lister      lists directories
     * @param subject     the user acting on the uploads
     * @param restriction restricts what the user may do
     */
    S3MultipartUploads(PnfsHandler pnfs, DirectoryListSource lister, Subject subject,
          Restriction restriction) {
        this.pnfs = requireNonNull(pnfs);
        this.lister = requireNonNull(lister);
        this.subject = requireNonNull(subject);
        this.restriction = requireNonNull(restriction);
    }

    /**
     * Returns the directory holding the uploads of a bucket.
     */
    static FsPath directory(FsPath bucket) {
        return bucket.child(S3Paths.MULTIPART_DIRECTORY);
    }

    /**
     * Returns the part number a file name stands for, or empty if it names no part.
     */
    static Optional<Integer> partNumber(String name) {
        if (!PART_NUMBER.matcher(name).matches()) {
            return Optional.empty();
        }
        int number = Integer.parseInt(name);
        return number <= MAX_PART_NUMBER ? Optional.of(number) : Optional.empty();
    }

    /**
     * Starts an upload.
     *
     * @param xattrs the object's metadata
     */
    Upload create(FsPath bucket, String key, Map<String, String> xattrs) throws CacheException {
        FsPath uploads = directory(bucket);
        try {
            pnfs.createPnfsDirectory(uploads.toString());
        } catch (FileExistsCacheException e) {
            // Other uploads are in progress.
        }
        String id = UUID.randomUUID().toString();
        var uploadXattrs = new HashMap<>(xattrs);
        uploadXattrs.put(XATTR_KEY, key);
        FileAttributes attributes = FileAttributes.ofFileType(FileType.DIR);
        attributes.setXattrs(uploadXattrs);
        FsPath directory = uploads.child(id);
        FileAttributes created = pnfs.createPnfsDirectory(directory.toString(), attributes)
              .getFileAttributes();
        Instant initiated = created != null && created.isDefined(CREATION_TIME)
              ? Instant.ofEpochMilli(created.getCreationTime()) : Instant.now();
        return new Upload(id, key, initiated, Map.copyOf(xattrs), directory);
    }

    /**
     * Returns an upload of an object, or empty if the bucket has no such upload.
     */
    Optional<Upload> find(FsPath bucket, String id, String key) throws CacheException {
        if (!UPLOAD_ID.matcher(id).matches()) {
            return Optional.empty();
        }
        FsPath directory = directory(bucket).child(id);
        try {
            FileAttributes attributes = pnfs.getFileAttributes(directory, UPLOAD_ATTRIBUTES);
            return toUpload(id, directory, attributes).filter(u -> u.key().equals(key));
        } catch (FileNotFoundCacheException | NotDirCacheException e) {
            return Optional.empty();
        }
    }

    /**
     * Returns the uploads of a bucket, sorted by key and then by when they were started.
     */
    List<Upload> list(FsPath bucket) throws CacheException, InterruptedException {
        FsPath uploads = directory(bucket);
        var result = new ArrayList<Upload>();
        for (DirectoryEntry entry : read(uploads, UPLOAD_ATTRIBUTES)) {
            toUpload(entry.getName(), uploads.child(entry.getName()), entry.getFileAttributes())
                  .ifPresent(result::add);
        }
        result.sort(Comparator.comparing(Upload::key).thenComparing(Upload::initiated));
        return result;
    }

    /**
     * Returns the parts of an upload by part number.
     *
     * @param attributes the attributes to fetch for each part
     */
    SortedMap<Integer, FileAttributes> parts(Upload upload, Set<FileAttribute> attributes)
          throws CacheException, InterruptedException {
        var parts = new TreeMap<Integer, FileAttributes>();
        for (DirectoryEntry entry : read(upload.directory(), attributes)) {
            if (entry.getFileAttributes().getFileType() == FileType.REGULAR) {
                partNumber(entry.getName()).ifPresent(
                      n -> parts.put(n, entry.getFileAttributes()));
            }
        }
        return parts;
    }

    /**
     * Removes an upload with everything it holds.  An upload already gone is no error.
     */
    void remove(FsPath directory) throws CacheException, InterruptedException {
        for (DirectoryEntry entry : read(directory, ENTRY_ATTRIBUTES)) {
            try {
                pnfs.deletePnfsEntry(directory.child(entry.getName()).toString(),
                      EnumSet.of(FileType.REGULAR, FileType.LINK));
            } catch (FileNotFoundCacheException e) {
                // Removed concurrently.
            }
        }
        try {
            pnfs.deletePnfsEntry(directory.toString(), EnumSet.of(FileType.DIR));
        } catch (FileNotFoundCacheException e) {
            // Removed concurrently.
        }
    }

    /**
     * Removes the uploads of a bucket that were started before a point in time, including
     * directories in the upload area that are not uploads.  Failures to remove an upload are
     * logged and do not stop the others from being removed.
     *
     * @return the number of uploads removed
     */
    int sweep(FsPath bucket, Instant startedBefore) throws CacheException, InterruptedException {
        FsPath uploads = directory(bucket);
        int removed = 0;
        for (DirectoryEntry entry : read(uploads, UPLOAD_ATTRIBUTES)) {
            FileAttributes attributes = entry.getFileAttributes();
            if (attributes.getFileType() != FileType.DIR
                  || !Instant.ofEpochMilli(attributes.getCreationTime()).isBefore(startedBefore)) {
                continue;
            }
            FsPath directory = uploads.child(entry.getName());
            try {
                remove(directory);
                removed++;
                LOGGER.info("Removed multipart upload {} started at {}", directory,
                      Instant.ofEpochMilli(attributes.getCreationTime()));
            } catch (CacheException e) {
                LOGGER.warn("Failed to remove multipart upload {}: {}", directory,
                      e.getMessage());
            }
        }
        return removed;
    }

    /**
     * Returns the entries of a directory; none if it does not exist.
     */
    private List<DirectoryEntry> read(FsPath directory, Set<FileAttribute> attributes)
          throws CacheException, InterruptedException {
        var entries = new ArrayList<DirectoryEntry>();
        try (DirectoryStream stream = lister.list(subject, restriction, directory, null,
              Range.all(), attributes)) {
            stream.forEach(entries::add);
        } catch (FileNotFoundCacheException | NotDirCacheException e) {
            return List.of();
        }
        return entries;
    }

    private static Optional<Upload> toUpload(String id, FsPath directory,
          FileAttributes attributes) {
        if (attributes.getFileType() != FileType.DIR || !UPLOAD_ID.matcher(id).matches()
              || !attributes.isDefined(XATTR)) {
            return Optional.empty();
        }
        var xattrs = new HashMap<>(attributes.getXattrs());
        String key = xattrs.remove(XATTR_KEY);
        if (key == null) {
            return Optional.empty();
        }
        xattrs.keySet().removeIf(name -> !name.startsWith("s3."));
        return Optional.of(new Upload(id, key, Instant.ofEpochMilli(attributes.getCreationTime()),
              Map.copyOf(xattrs), directory));
    }
}
