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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.namespace.FileAttribute.CREATION_TIME;
import static org.dcache.namespace.FileAttribute.LOCATIONS;
import static org.dcache.namespace.FileAttribute.MODIFICATION_TIME;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.namespace.FileAttribute.XATTR;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListSource;
import org.dcache.util.list.DirectoryStream;
import org.dcache.vehicles.FileAttributes;
import org.gaul.s3proxy.S3ErrorCode;
import org.gaul.s3proxy.S3ProxyException;
import org.gaul.s3proxy.blobstore.BlobStore;
import org.gaul.s3proxy.blobstore.ContentMetadata;
import org.gaul.s3proxy.blobstore.S3Exceptions;
import org.gaul.s3proxy.blobstore.SdkRequests;
import org.gaul.s3proxy.blobstore.SdkResponses;
import org.gaul.s3proxy.blobstore.domain.MultipartUpload;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.RestoreObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * An s3proxy blob store over the dCache namespace, acting as one user for one request.
 * <p>
 * Buckets are the directories below the user's bucket root and objects the regular files below a
 * bucket (see {@link S3Paths}).  Files whose only copy is on tape (no pool has a replica) are
 * reported with storage class {@code GLACIER} and cannot be read until they are restored.
 * <p>
 * The ETag of a file is its MD5 checksum if dCache has one.  Otherwise it is an opaque value
 * derived from the file's identity and modification time, with a {@code -1} suffix that tells
 * clients, as for multipart uploads, not to compare it with an MD5 of the content.
 * <p>
 * {@code GLACIER} files are restored by {@link S3ObjectRestorer}; while that is in progress, HEAD
 * answers with {@code x-amz-restore: ongoing-request="true"}.
 * <p>
 * An upload writes to a hidden file next to the object's file and renames it over the object
 * once the data is complete, so readers see either the old or the new object.  The door
 * computes the MD5 checksum of the data, which becomes the ETag and is stored as the file's
 * checksum.  Content type and user metadata are stored as extended attributes.  Deleting an
 * object keeps its parent directories.  Multipart uploads are not supported yet.
 */
public class DcacheBlobStore implements BlobStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DcacheBlobStore.class);

    /**
     * The most keys S3 returns in one listing page.
     */
    static final int MAX_KEYS = 1000;

    /**
     * The extended attribute holding an object's content type.
     */
    static final String XATTR_CONTENT_TYPE = "s3.content-type";

    /**
     * The prefix of the extended attributes holding an object's user metadata.
     */
    static final String XATTR_USER_METADATA_PREFIX = "s3.meta.";

    private static final long MINIMUM_MULTIPART_PART_SIZE = 5L * 1024 * 1024;

    // Sent to PnfsManager, so they must be serializable: Sets.union views are not.
    private static final Set<FileAttribute> LIST_ATTRIBUTES = Sets.immutableEnumSet(
          TYPE, PNFSID, SIZE, MODIFICATION_TIME, CHECKSUM, LOCATIONS, STORAGEINFO);

    private static final Set<FileAttribute> TYPE_ATTRIBUTES = Sets.immutableEnumSet(TYPE);

    private static final Set<FileAttribute> OBJECT_ATTRIBUTES = Sets.immutableEnumSet(
          Sets.union(LIST_ATTRIBUTES, EnumSet.of(XATTR)));

    private final Supplier<S3User> user;
    private final PnfsHandler pnfs;
    private final DirectoryListSource lister;
    private final S3ObjectReader reader;
    private final S3ObjectWriter writer;
    private final S3ObjectRestorer restorer;
    private final int maxListEntries;

    /**
     * @param user           logs in the client on first use
     * @param pnfs           the door's PnfsHandler; requests run as {@code user}
     * @param lister         lists directories
     * @param reader         reads file content
     * @param writer         writes file content
     * @param restorer       brings files back from tape
     * @param maxListEntries how many directory entries a listing without delimiter reads before
     *                       it ends the page early
     */
    public DcacheBlobStore(Supplier<S3User> user, PnfsHandler pnfs, DirectoryListSource lister,
          S3ObjectReader reader, S3ObjectWriter writer, S3ObjectRestorer restorer,
          int maxListEntries) {
        this.user = requireNonNull(user);
        this.pnfs = requireNonNull(pnfs);
        this.lister = requireNonNull(lister);
        this.reader = requireNonNull(reader);
        this.writer = requireNonNull(writer);
        this.restorer = requireNonNull(restorer);
        this.maxListEntries = maxListEntries;
    }

    private PnfsHandler pnfs(S3User user) {
        return new PnfsHandler(pnfs, user.subject(), user.restriction());
    }

    // Buckets

    @Override
    public ListBucketsResponse list() {
        S3User u = user.get();
        var buckets = new ArrayList<Bucket>();
        try (DirectoryStream stream = lister.list(u.subject(), u.restriction(), u.root(), null,
              Range.all(), EnumSet.of(TYPE, CREATION_TIME))) {
            for (DirectoryEntry entry : stream) {
                FileAttributes attributes = entry.getFileAttributes();
                if (attributes.getFileType() == FileType.DIR
                      && S3Paths.isBucketName(entry.getName())) {
                    buckets.add(SdkResponses.bucket(entry.getName(),
                          Instant.ofEpochMilli(attributes.getCreationTime())));
                }
            }
        } catch (CacheException e) {
            throw S3Errors.from(e, "", null);
        } catch (InterruptedException e) {
            throw S3Errors.interrupted(e);
        }
        buckets.sort(Comparator.comparing(Bucket::name));
        return ListBucketsResponse.builder().buckets(buckets).build();
    }

    @Override
    public HeadBucketResponse headBucket(HeadBucketRequest request) {
        S3User u = user.get();
        bucketDirectory(u, request.bucket());
        return HeadBucketResponse.builder().build();
    }

    @Override
    public BucketCannedACL getContainerAccess(String container) {
        S3User u = user.get();
        bucketDirectory(u, container);
        return BucketCannedACL.PRIVATE;
    }

    @Override
    public void setContainerAccess(String container, BucketCannedACL access) {
        if (access != BucketCannedACL.PRIVATE) {
            throw notImplemented();
        }
        getContainerAccess(container);
    }

    @Override
    public CreateBucketResponse createContainer(CreateBucketRequest request) {
        if (request.acl() != null && request.acl() != BucketCannedACL.PRIVATE) {
            throw notImplemented();
        }
        S3User u = user.get();
        String bucket = request.bucket();
        FsPath path = S3Paths.bucket(u.root(), bucket);
        try {
            pnfs(u).createPnfsDirectory(path.toString());
        } catch (FileExistsCacheException e) {
            throw S3Exceptions.bucketAlreadyOwnedByYou(bucket);
        } catch (PermissionDeniedCacheException e) {
            // Chimera checks permissions first.  Clients create buckets they already have
            // (rclone before each upload), and expect to be told so.
            if (containerExists(bucket)) {
                throw S3Exceptions.bucketAlreadyOwnedByYou(bucket);
            }
            throw S3Errors.from(e, bucket, null);
        } catch (CacheException e) {
            throw S3Errors.from(e, bucket, null);
        }
        return CreateBucketResponse.builder().location("/" + bucket).build();
    }

    /**
     * Removes a bucket that holds no files.  Directories left behind by deleted objects do not
     * count: they are removed with the bucket.
     */
    @Override
    public void deleteBucket(String container) {
        S3User u = user.get();
        FsPath path = S3Paths.bucket(u.root(), container);
        try {
            var directories = new ArrayList<FsPath>();
            if (!collectEmptyTree(u, container, path, directories)) {
                return;
            }
            for (FsPath directory : directories) {
                try {
                    pnfs(u).deletePnfsEntry(directory.toString(), EnumSet.of(FileType.DIR));
                } catch (FileNotFoundCacheException e) {
                    // Removed concurrently; what matters is that it is gone.
                } catch (PermissionDeniedCacheException e) {
                    throw e;
                } catch (CacheException e) {
                    // Chimera has no distinct error for a directory that is not empty.
                    if (!read(u, directory, TYPE_ATTRIBUTES).isEmpty()) {
                        throw S3Exceptions.bucketNotEmpty(container);
                    }
                    throw e;
                }
            }
        } catch (CacheException e) {
            throw S3Errors.from(e, container, null);
        } catch (InterruptedException e) {
            throw S3Errors.interrupted(e);
        }
    }

    /**
     * Adds the directories below and including {@code directory} to {@code directories},
     * children before their parents.
     *
     * @return false if the directory does not exist
     * @throws software.amazon.awssdk.services.s3.model.S3Exception BucketNotEmpty if the tree
     *                                                              holds anything but directories
     */
    private boolean collectEmptyTree(S3User u, String bucket, FsPath directory,
          List<FsPath> directories) throws CacheException, InterruptedException {
        List<DirectoryEntry> entries;
        try {
            entries = read(u, directory, TYPE_ATTRIBUTES);
        } catch (FileNotFoundCacheException | NotDirCacheException e) {
            return false;
        }
        for (DirectoryEntry entry : entries) {
            if (entry.getFileAttributes().getFileType() != FileType.DIR) {
                throw S3Exceptions.bucketNotEmpty(bucket);
            }
            collectEmptyTree(u, bucket, directory.child(entry.getName()), directories);
        }
        directories.add(directory);
        return true;
    }

    private List<DirectoryEntry> read(S3User u, FsPath directory, Set<FileAttribute> attributes)
          throws CacheException, InterruptedException {
        var entries = new ArrayList<DirectoryEntry>();
        try (DirectoryStream stream = lister.list(u.subject(), u.restriction(), directory, null,
              Range.all(), attributes)) {
            stream.forEach(entries::add);
        }
        return entries;
    }

    /**
     * Returns the directory of an existing bucket.
     */
    private FsPath bucketDirectory(S3User u, String bucket) {
        FsPath path = S3Paths.bucket(u.root(), bucket);
        try {
            FileAttributes attributes = pnfs(u).getFileAttributes(path, EnumSet.of(TYPE));
            if (attributes.getFileType() != FileType.DIR) {
                throw S3Exceptions.noSuchBucket(bucket, "not a directory");
            }
            return path;
        } catch (CacheException e) {
            throw S3Errors.from(e, bucket, null);
        }
    }

    // Objects

    @Override
    public HeadObjectResponse blobMetadata(HeadObjectRequest request) {
        if (request.versionId() != null) {
            throw new UnsupportedOperationException("versioning not supported");
        }
        S3User u = user.get();
        FileAttributes attributes = objectAttributes(u, request.bucket(), request.key());
        boolean isOnTapeOnly = isOnTapeOnly(attributes);
        return HeadObjectResponse.builder()
              .acceptRanges("bytes")
              .contentLength(attributes.getSize())
              .contentType(contentType(attributes))
              .eTag(eTag(attributes))
              .lastModified(lastModified(attributes))
              .metadata(userMetadata(attributes))
              .storageClass(isOnTapeOnly ? StorageClass.GLACIER : StorageClass.STANDARD)
              .restore(isOnTapeOnly && isRestoring(u, attributes)
                    ? "ongoing-request=\"true\"" : null)
              .build();
    }

    /**
     * Whether a tape-only file is being restored.  Failing to find out is not worth failing a
     * HEAD request for, so it counts as not restoring.
     */
    private boolean isRestoring(S3User u, FileAttributes attributes) {
        try {
            return restorer.isRestoring(u, attributes);
        } catch (CacheException e) {
            LOGGER.debug("Failed to query restore of {}: {}", attributes.getPnfsId(),
                  e.getMessage());
            return false;
        } catch (InterruptedException e) {
            throw S3Errors.interrupted(e);
        }
    }

    /**
     * Restores a tape-only file for the request's Days, one day if it names none.  A file with a
     * copy on disk counts as restored.
     */
    @Override
    public RestoreObjectResponse restoreObject(RestoreObjectRequest request) {
        if (request.versionId() != null) {
            throw new UnsupportedOperationException("versioning not supported");
        }
        S3User u = user.get();
        String bucket = request.bucket();
        String key = request.key();
        FileAttributes attributes = objectAttributes(u, bucket, key);
        if (!isOnTapeOnly(attributes)) {
            return restoreResponse(200);
        }

        Integer days = request.restoreRequest() == null ? null : request.restoreRequest().days();
        FsPath path = S3Paths.object(S3Paths.bucket(u.root(), bucket), key);
        try {
            if (restorer.isRestoring(u, attributes)) {
                throw S3Errors.restoreAlreadyInProgress();
            }
            restorer.restore(u, path, attributes, days == null ? 1 : days);
        } catch (CacheException e) {
            throw S3Errors.from(e, bucket, key);
        } catch (InterruptedException e) {
            throw S3Errors.interrupted(e);
        }
        return restoreResponse(202);
    }

    private static RestoreObjectResponse restoreResponse(int status) {
        return (RestoreObjectResponse) RestoreObjectResponse.builder()
              .sdkHttpResponse(SdkHttpResponse.builder().statusCode(status).build())
              .build();
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getBlob(GetObjectRequest request) {
        if (request.versionId() != null) {
            throw new UnsupportedOperationException("versioning not supported");
        }
        S3User u = user.get();
        String bucket = request.bucket();
        String key = request.key();
        FileAttributes attributes = objectAttributes(u, bucket, key);
        String eTag = eTag(attributes);
        checkConditions(request, eTag, lastModified(attributes));

        if (isOnTapeOnly(attributes)) {
            throw S3Errors.invalidObjectState();
        }

        long size = attributes.getSize();
        long offset = 0;
        long length = size;
        String contentRange = null;
        var range = SdkRequests.parseRange(request.range());
        if (range != null) {
            long last;
            if (range.first() == null) {
                offset = Math.max(0, size - requireNonNull(range.last()));
                last = size - 1;
            } else {
                offset = range.first();
                last = range.last() == null ? size - 1 : Math.min(range.last(), size - 1);
            }
            if (offset >= size || offset > last) {
                throw S3Exceptions.fromStatusCode(416);
            }
            length = last - offset + 1;
            contentRange = "bytes " + offset + "-" + last + "/" + size;
        }

        var response = GetObjectResponse.builder()
              .acceptRanges("bytes")
              .contentLength(length)
              .contentRange(contentRange)
              .contentType(contentType(attributes))
              .eTag(eTag)
              .lastModified(lastModified(attributes))
              .metadata(userMetadata(attributes))
              .storageClass(storageClass(attributes))
              .build();

        if (length == 0) {
            return SdkResponses.getResponse(response, InputStream.nullInputStream());
        }

        FsPath path = S3Paths.object(S3Paths.bucket(u.root(), bucket), key);
        try {
            return SdkResponses.getResponse(response,
                  reader.read(u, path, attributes, offset, length));
        } catch (CacheException e) {
            throw S3Errors.from(e, bucket, key);
        } catch (InterruptedException e) {
            throw S3Errors.interrupted(e);
        }
    }

    @Override
    public ObjectCannedACL getBlobAccess(String container, String name) {
        S3User u = user.get();
        objectAttributes(u, container, name);
        return ObjectCannedACL.PRIVATE;
    }

    @Override
    public void setBlobAccess(String container, String name, ObjectCannedACL access) {
        if (access != ObjectCannedACL.PRIVATE) {
            throw notImplemented();
        }
        getBlobAccess(container, name);
    }

    @Override
    public PutObjectResponse putBlob(PutObjectRequest request, InputStream payload) {
        try (payload) {
            if (request.acl() != null && request.acl() != ObjectCannedACL.PRIVATE) {
                throw notImplemented();
            }
            S3User u = user.get();
            String bucket = request.bucket();
            String key = request.key();
            FsPath path = S3Paths.object(bucketDirectory(u, bucket), key);
            byte[] contentMd5 = request.contentMD5() == null
                  ? null : Base64.getDecoder().decode(request.contentMD5());
            String eTag = store(u, bucket, key, path,
                  xattrs(request.contentType(), request.metadata()), payload,
                  request.contentLength(), contentMd5);
            return PutObjectResponse.builder().eTag(eTag).build();
        } catch (IOException e) {
            // Reading the client's data failed.
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Copies an object through the door: its data is read from a pool and written as a new
     * file.  Copying an object onto itself replaces its metadata the same way.
     */
    @Override
    public CopyObjectResponse copyBlob(CopyObjectRequest request) {
        if (request.sourceVersionId() != null) {
            throw new UnsupportedOperationException("versioning not supported");
        }
        if (request.acl() != null && request.acl() != ObjectCannedACL.PRIVATE) {
            throw notImplemented();
        }
        S3User u = user.get();
        String sourceBucket = request.sourceBucket();
        String sourceKey = request.sourceKey();
        FileAttributes source = objectAttributes(u, sourceBucket, sourceKey);
        checkCopySourceConditions(request, eTag(source), lastModified(source));
        if (isOnTapeOnly(source)) {
            throw S3Errors.invalidObjectState();
        }

        String bucket = request.destinationBucket();
        String key = request.destinationKey();
        FsPath path = S3Paths.object(bucketDirectory(u, bucket), key);
        Map<String, String> xattrs = request.metadataDirective() == MetadataDirective.REPLACE
              ? xattrs(request.contentType(), request.metadata())
              : objectXattrs(source);

        FsPath sourcePath = S3Paths.object(S3Paths.bucket(u.root(), sourceBucket), sourceKey);
        long size = source.getSize();
        String eTag;
        try (InputStream data = size == 0
              ? InputStream.nullInputStream()
              : reader.read(u, sourcePath, source, 0, size)) {
            eTag = store(u, bucket, key, path, xattrs, data, size, null);
        } catch (CacheException e) {
            throw S3Errors.from(e, sourceBucket, sourceKey);
        } catch (InterruptedException e) {
            throw S3Errors.interrupted(e);
        } catch (IOException e) {
            throw S3Errors.serviceUnavailable("Failed to read " + sourceKey + ": "
                  + e.getMessage(), e);
        }
        // Without LastModified, the frontend reads it from the new object.
        return CopyObjectResponse.builder()
              .copyObjectResult(CopyObjectResult.builder().eTag(eTag).build())
              .build();
    }

    /**
     * Writes an object.  The data goes to a new hidden file in the object's directory, which
     * replaces the object's file once the data is complete and its MD5 checksum is recorded.
     *
     * @param contentMd5 the MD5 checksum the client expects, or null
     * @return the quoted ETag of the object
     * @throws IOException if reading {@code data} failed
     */
    private String store(S3User u, String bucket, String key, FsPath path,
          Map<String, String> xattrs, InputStream data, long length, byte @Nullable [] contentMd5)
          throws IOException {
        FsPath upload = path.parent().child(S3Paths.UPLOAD_PREFIX + UUID.randomUUID());
        MessageDigest md5 = md5();
        PnfsId pnfsId;
        try {
            pnfsId = writer.write(u, upload, path, xattrs, new DigestInputStream(data, md5),
                  length);
        } catch (CacheException e) {
            throw writeError(e, bucket, key);
        } catch (InterruptedException e) {
            throw S3Errors.interrupted(e);
        }

        boolean isStored = false;
        try {
            byte[] digest = md5.digest();
            if (contentMd5 != null && !MessageDigest.isEqual(contentMd5, digest)) {
                throw new S3ProxyException(S3ErrorCode.BAD_DIGEST);
            }
            Checksum checksum = new Checksum(ChecksumType.MD5_TYPE, digest);
            // The door's own record of what it received, so it does not depend on the user's
            // permission to change the file's attributes.
            pnfs.setFileAttributes(pnfsId, FileAttributes.ofChecksum(checksum));
            pnfs(u).renameEntry(pnfsId, upload.toString(), path.toString(), true);
            isStored = true;
            return '"' + checksum.getValue() + '"';
        } catch (CacheException e) {
            throw writeError(e, bucket, key);
        } finally {
            if (!isStored) {
                removeUpload(u, pnfsId, upload);
            }
        }
    }

    /**
     * Removes the file of a failed upload.  As for other doors, this is done without the user's
     * restriction, which may allow uploading but not deleting.
     */
    private void removeUpload(S3User u, PnfsId pnfsId, FsPath upload) {
        try {
            new PnfsHandler(pnfs, u.subject(), Restrictions.none())
                  .deletePnfsEntry(pnfsId, upload.toString());
        } catch (CacheException e) {
            LOGGER.warn("Failed to remove {} ({}) after failed upload: {}", upload, pnfsId,
                  e.getMessage());
        }
    }

    /**
     * Maps a failed write.  dCache cannot hold both a key and keys below it ({@code a} and
     * {@code a/b}), so writing either when the other exists fails.
     */
    private static RuntimeException writeError(CacheException e, String bucket, String key) {
        if (e instanceof NotDirCacheException || e instanceof NotFileCacheException
              || e instanceof FileExistsCacheException) {
            return S3Exceptions.invalidArgument(
                  "Object key " + key + " conflicts with an existing object or prefix");
        }
        return S3Errors.from(e, bucket, key);
    }

    private static MessageDigest md5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 is not supported", e);
        }
    }

    /**
     * Applies the conditions of a copy on its source the way S3 does: each failed condition is
     * a 412, except that a matching If-Match overrides a failed If-Unmodified-Since, and a
     * failed If-None-Match overrides If-Modified-Since.
     */
    private static void checkCopySourceConditions(CopyObjectRequest request, String eTag,
          Instant lastModified) {
        String ifMatch = request.copySourceIfMatch();
        String ifNoneMatch = request.copySourceIfNoneMatch();
        if (ifMatch != null && !ifMatch.equals("*") && !eTag.equals(quoted(ifMatch))) {
            throw conditionFailed(412, eTag);
        }
        if (ifNoneMatch != null && (ifNoneMatch.equals("*") || eTag.equals(quoted(ifNoneMatch)))) {
            throw conditionFailed(412, eTag);
        }
        Instant ifModifiedSince = request.copySourceIfModifiedSince();
        if (ifModifiedSince != null && ifNoneMatch == null
              && !lastModified.isAfter(ifModifiedSince)) {
            throw conditionFailed(412, eTag);
        }
        Instant ifUnmodifiedSince = request.copySourceIfUnmodifiedSince();
        if (ifUnmodifiedSince != null && ifMatch == null
              && lastModified.isAfter(ifUnmodifiedSince)) {
            throw conditionFailed(412, eTag);
        }
    }

    /**
     * Deletes an object.  Deleting a missing object succeeds, as in S3; its parent directories
     * are kept.
     */
    @Override
    public DeleteObjectResponse removeBlob(DeleteObjectRequest request) {
        if (request.versionId() != null) {
            throw new UnsupportedOperationException("versioning not supported");
        }
        S3User u = user.get();
        String bucket = request.bucket();
        String key = request.key();
        FsPath path = S3Paths.object(S3Paths.bucket(u.root(), bucket), key);
        try {
            pnfs(u).deletePnfsEntry(path.toString(), EnumSet.of(FileType.REGULAR));
        } catch (FileNotFoundCacheException | NotDirCacheException | NotFileCacheException e) {
            bucketDirectory(u, bucket);
        } catch (CacheException e) {
            throw S3Errors.from(e, bucket, key);
        }
        return DeleteObjectResponse.builder().build();
    }

    /**
     * Deletes objects in parallel threads, which do not know the client's address; so the
     * client is logged in first.
     */
    @Override
    public DeleteObjectsResponse removeBlobs(DeleteObjectsRequest request) {
        user.get();
        return BlobStore.super.removeBlobs(request);
    }

    /**
     * Returns the attributes of an object, telling a missing bucket apart from a missing key.
     */
    private FileAttributes objectAttributes(S3User u, String bucket, String key) {
        FsPath bucketPath = S3Paths.bucket(u.root(), bucket);
        FsPath path = S3Paths.object(bucketPath, key);
        try {
            FileAttributes attributes = pnfs(u).getFileAttributes(path, OBJECT_ATTRIBUTES);
            if (attributes.getFileType() != FileType.REGULAR) {
                throw S3Exceptions.noSuchKey(bucket, key, "not a file");
            }
            return attributes;
        } catch (FileNotFoundCacheException | NotDirCacheException e) {
            bucketDirectory(u, bucket);
            throw S3Exceptions.noSuchKey(bucket, key, e.getMessage(), e);
        } catch (CacheException e) {
            throw S3Errors.from(e, bucket, key);
        }
    }

    /**
     * Applies the conditional headers of a GET the way S3 does: a failed If-Match or
     * If-Unmodified-Since is a 412, a matching If-None-Match or failed If-Modified-Since a 304.
     */
    private static void checkConditions(GetObjectRequest request, String eTag,
          Instant lastModified) {
        String ifMatch = request.ifMatch();
        String ifNoneMatch = request.ifNoneMatch();
        if (ifMatch != null && !ifMatch.equals("*") && !eTag.equals(quoted(ifMatch))) {
            throw conditionFailed(412, eTag);
        }
        if (ifNoneMatch != null && (ifNoneMatch.equals("*") || eTag.equals(quoted(ifNoneMatch)))) {
            throw conditionFailed(304, eTag);
        }
        Instant ifModifiedSince = request.ifModifiedSince();
        if (ifModifiedSince != null && ifNoneMatch == null
              && !lastModified.isAfter(ifModifiedSince)) {
            throw conditionFailed(304, eTag);
        }
        Instant ifUnmodifiedSince = request.ifUnmodifiedSince();
        if (ifUnmodifiedSince != null && ifMatch == null
              && lastModified.isAfter(ifUnmodifiedSince)) {
            throw conditionFailed(412, eTag);
        }
    }

    private static RuntimeException conditionFailed(int status, String eTag) {
        return S3Exceptions.fromStatusCode(status, eTag, Map.of(), null);
    }

    private static String quoted(String eTag) {
        return eTag.startsWith("\"") ? eTag : '"' + eTag + '"';
    }

    // Listing

    @Override
    public ListObjectsV2Response list(ListObjectsV2Request request) {
        S3User u = user.get();
        String bucket = request.bucket();
        FsPath bucketPath = S3Paths.bucket(u.root(), bucket);

        String delimiter = request.delimiter();
        if (delimiter != null && delimiter.isEmpty()) {
            delimiter = null;
        }
        if (delimiter != null && !delimiter.equals("/")) {
            throw new S3ProxyException(S3ErrorCode.NOT_IMPLEMENTED,
                  "Only '/' is supported as delimiter");
        }

        int maxKeys = request.maxKeys() == null ? MAX_KEYS : Math.min(request.maxKeys(), MAX_KEYS);
        String marker = request.continuationToken() != null
              ? request.continuationToken() : request.startAfter();
        String prefix = request.prefix() == null ? "" : request.prefix();
        String directoryKey = prefix.substring(0, prefix.lastIndexOf('/') + 1);
        String namePrefix = prefix.substring(directoryKey.length());

        var listing = new Listing(u, bucket, marker, maxKeys, delimiter != null);
        try {
            var directory = S3Paths.directory(bucketPath, directoryKey);
            if (directory.isEmpty() || maxKeys == 0
                  || !listing.walk(directory.get(), directoryKey, namePrefix)) {
                // Nothing to list; tell an empty listing apart from a missing bucket.
                bucketDirectory(u, bucket);
            }
        } catch (CacheException e) {
            throw S3Errors.from(e, bucket, null);
        } catch (InterruptedException e) {
            throw S3Errors.interrupted(e);
        }
        return listing.page();
    }

    /**
     * One page of a bucket listing.  Siblings are visited in the order of their keys -- a
     * directory sorts by its name with a trailing {@code /} -- and directories are descended into
     * depth first, so entries are found in key order and the walk stops once the page is full.
     * Subtrees whose keys all sort before the marker are not listed.
     */
    private class Listing {

        private final S3User user;
        private final String bucket;
        private final @Nullable String marker;
        private final int maxKeys;
        private final boolean delimited;
        private final List<S3Object> contents = new ArrayList<>();
        private final List<CommonPrefix> prefixes = new ArrayList<>();
        private @Nullable String lastKey;
        private boolean truncated;
        private int entriesRead;

        Listing(S3User user, String bucket, @Nullable String marker, int maxKeys,
              boolean delimited) {
            this.user = user;
            this.bucket = bucket;
            this.marker = marker;
            this.maxKeys = maxKeys;
            this.delimited = delimited;
        }

        /**
         * Adds the entries below a directory whose names start with {@code namePrefix}.
         *
         * @return false if the directory does not exist
         */
        boolean walk(FsPath directory, String directoryKey, String namePrefix)
              throws CacheException, InterruptedException {
            if (lastKey != null && entriesRead > maxListEntries) {
                // A short page is a valid page; the client continues after the last key.
                truncated = true;
                return true;
            }

            List<DirectoryEntry> entries;
            try {
                entries = read(directory);
            } catch (FileNotFoundCacheException | NotDirCacheException e) {
                return false;
            }

            var children = new ArrayList<Child>();
            for (DirectoryEntry entry : entries) {
                String name = entry.getName();
                if (!name.startsWith(namePrefix) || S3Paths.isReserved(name)) {
                    continue;
                }
                switch (entry.getFileAttributes().getFileType()) {
                    case DIR -> children.add(new Child(directoryKey + name + '/', entry));
                    case REGULAR -> children.add(new Child(directoryKey + name, entry));
                    default -> {
                    }
                }
            }
            children.sort(Comparator.comparing(Child::key));

            for (Child child : children) {
                if (truncated) {
                    break;
                }
                String key = child.key();
                if (child.entry().getFileAttributes().getFileType() == FileType.DIR) {
                    if (delimited) {
                        if (isAfterMarker(key)) {
                            add(key, null);
                        }
                    } else if (mayContainKeysAfterMarker(key)) {
                        walk(directory.child(child.entry().getName()), key, "");
                    }
                } else if (isAfterMarker(key)) {
                    add(key, child.entry().getFileAttributes());
                }
            }
            return true;
        }

        private List<DirectoryEntry> read(FsPath directory)
              throws CacheException, InterruptedException {
            List<DirectoryEntry> entries = DcacheBlobStore.this.read(user, directory,
                  LIST_ATTRIBUTES);
            entriesRead += entries.size();
            return entries;
        }

        private boolean isAfterMarker(String key) {
            return marker == null || key.compareTo(marker) > 0;
        }

        /**
         * Whether a directory, whose keys all start with {@code directoryKey}, may hold keys after
         * the marker.
         */
        private boolean mayContainKeysAfterMarker(String directoryKey) {
            return marker == null || directoryKey.compareTo(marker) > 0
                  || marker.startsWith(directoryKey);
        }

        private void add(String key, @Nullable FileAttributes attributes) {
            if (contents.size() + prefixes.size() == maxKeys) {
                truncated = true;
                return;
            }
            if (attributes == null) {
                prefixes.add(SdkResponses.commonPrefix(key));
            } else {
                contents.add(SdkResponses.objectEntry(key, eTag(attributes),
                      lastModified(attributes), attributes.getSize(),
                      storageClass(attributes)));
            }
            lastKey = key;
        }

        ListObjectsV2Response page() {
            return SdkResponses.objectsPage(contents, prefixes, truncated ? lastKey : null);
        }
    }

    private record Child(String key, DirectoryEntry entry) {
    }

    // Object attributes

    /**
     * Whether the only copy of a file is on tape: no pool has a replica, so reading it means
     * staging it.  A replica on any pool counts, as PoolManager copies it to a pool the client
     * may read from.
     */
    static boolean isOnTapeOnly(FileAttributes attributes) {
        return attributes.isDefined(LOCATIONS) && attributes.getLocations().isEmpty()
              && attributes.isDefined(STORAGEINFO) && attributes.getStorageInfo().isStored();
    }

    private static StorageClass storageClass(FileAttributes attributes) {
        return isOnTapeOnly(attributes) ? StorageClass.GLACIER : StorageClass.STANDARD;
    }

    private static Instant lastModified(FileAttributes attributes) {
        return Instant.ofEpochMilli(attributes.getModificationTime())
              .truncatedTo(ChronoUnit.SECONDS);
    }

    /**
     * Returns the quoted ETag of a file: its MD5 checksum, or an opaque value that changes with
     * the file's content.
     */
    static String eTag(FileAttributes attributes) {
        if (attributes.isDefined(CHECKSUM)) {
            for (Checksum checksum : attributes.getChecksums()) {
                if (checksum.getType() == ChecksumType.MD5_TYPE) {
                    return '"' + checksum.getValue() + '"';
                }
            }
        }
        String identity = attributes.getPnfsId() + ":" + attributes.getModificationTime();
        return '"' + Hashing.murmur3_128().hashString(identity, UTF_8).toString() + "-1\"";
    }

    private static String contentType(FileAttributes attributes) {
        if (attributes.isDefined(XATTR)) {
            String contentType = attributes.getXattrs().get(XATTR_CONTENT_TYPE);
            if (contentType != null) {
                return contentType;
            }
        }
        return ContentMetadata.DEFAULT_CONTENT_TYPE;
    }

    /**
     * Returns the extended attributes that store an object's content type and user metadata.
     */
    private static Map<String, String> xattrs(@Nullable String contentType,
          Map<String, String> userMetadata) {
        var xattrs = new HashMap<String, String>();
        if (contentType != null && !contentType.equals(ContentMetadata.DEFAULT_CONTENT_TYPE)) {
            xattrs.put(XATTR_CONTENT_TYPE, contentType);
        }
        userMetadata.forEach((name, value) -> xattrs.put(XATTR_USER_METADATA_PREFIX + name, value));
        return xattrs;
    }

    /**
     * Returns the extended attributes of a file that store object metadata.
     */
    private static Map<String, String> objectXattrs(FileAttributes attributes) {
        var xattrs = new HashMap<String, String>();
        if (attributes.isDefined(XATTR)) {
            attributes.getXattrs().forEach((name, value) -> {
                if (name.equals(XATTR_CONTENT_TYPE)
                      || name.startsWith(XATTR_USER_METADATA_PREFIX)) {
                    xattrs.put(name, value);
                }
            });
        }
        return xattrs;
    }

    private static Map<String, String> userMetadata(FileAttributes attributes) {
        var metadata = new HashMap<String, String>();
        if (attributes.isDefined(XATTR)) {
            attributes.getXattrs().forEach((name, value) -> {
                if (name.startsWith(XATTR_USER_METADATA_PREFIX)) {
                    metadata.put(name.substring(XATTR_USER_METADATA_PREFIX.length()), value);
                }
            });
        }
        return metadata;
    }

    private static S3ProxyException notImplemented() {
        return new S3ProxyException(S3ErrorCode.NOT_IMPLEMENTED);
    }

    // Multipart uploads are not supported yet.

    @Override
    public MultipartUpload initiateMultipartUpload(CreateMultipartUploadRequest request) {
        throw notImplemented();
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        throw notImplemented();
    }

    @Override
    public CompleteMultipartUploadResponse completeMultipartUpload(MultipartUpload mpu,
          CompleteMultipartUploadRequest request) {
        throw notImplemented();
    }

    @Override
    public UploadPartResponse uploadMultipartPart(MultipartUpload mpu, UploadPartRequest request,
          InputStream is) {
        throw notImplemented();
    }

    @Override
    public List<Part> listMultipartUpload(MultipartUpload mpu) {
        throw notImplemented();
    }

    @Override
    public List<software.amazon.awssdk.services.s3.model.MultipartUpload> listMultipartUploads(
          String container) {
        headBucket(HeadBucketRequest.builder().bucket(container).build());
        return List.of();
    }

    @Override
    public long getMinimumMultipartPartSize() {
        return MINIMUM_MULTIPART_PART_SIZE;
    }
}
