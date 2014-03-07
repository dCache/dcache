/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.vehicles;

import javax.security.auth.Subject;

import java.util.Collections;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.namespace.CreateOption;

/**
 * Create a temporary path to which a file can be uploaded.
 *
 * This message enables writing a file without exposing the file before
 * the upload has completed. A temporary path is returned to which the
 * file can be written as if it had been written to the real path.
 *
 * At the end of the upload the temporary path is 'committed' to its final
 * location. The temporary upload path is also revocable.
 *
 * @see diskCacheV111.vehicles.PnfsCancelUpload
 * @see diskCacheV111.vehicles.PnfsCommitUpload
 */
public class PnfsCreateUploadPath extends PnfsMessage
{

    private final int uid;
    private final int gid;
    private final int mode;
    private final Long size;
    private final AccessLatency accessLatency;
    private final RetentionPolicy retentionPolicy;
    private final String spaceToken;
    private final Set<CreateOption> options;
    private String uploadPath;

    public PnfsCreateUploadPath(Subject subject, FsPath path, int uid, int gid, int mode, Long size,
                                AccessLatency accessLatency, RetentionPolicy retentionPolicy, String spaceToken,
                                Set<CreateOption> options)
    {
        this.uid = uid;
        this.gid = gid;
        this.mode = mode;
        this.size = size;
        this.accessLatency = accessLatency;
        this.retentionPolicy = retentionPolicy;
        this.spaceToken = spaceToken;
        this.options = options;
        setSubject(subject);
        setPnfsPath(path.toString());
        setReplyRequired(true);
    }

    public int getUid()
    {
        return uid;
    }

    public int getGid()
    {
        return gid;
    }

    public int getMode()
    {
        return mode;
    }

    public AccessLatency getAccessLatency()
    {
        return accessLatency;
    }

    public RetentionPolicy getRetentionPolicy()
    {
        return retentionPolicy;
    }

    public String getSpaceToken()
    {
        return spaceToken;
    }

    public Long getSize()
    {
        return size;
    }

    public Set<CreateOption> getOptions()
    {
        return Collections.unmodifiableSet(options);
    }

    public FsPath getPath()
    {
        return new FsPath(getPnfsPath());
    }

    public FsPath getUploadPath()
    {
        return uploadPath == null ? null : new FsPath(uploadPath);
    }

    public void setUploadPath(FsPath uploadPath)
    {
        this.uploadPath = uploadPath.toString();
    }
}
