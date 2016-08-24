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

import java.util.Collection;
import java.util.Set;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

import static java.util.Objects.requireNonNull;

/**
 * Revoke a temporary upload path.
 *
 * This message operates on a temporary upload path generated with
 * PnfsCreateUploadPath. The temporary upload path will be deleted
 * and the path will no longer be available for writing.
 */
public class PnfsCancelUpload extends PnfsMessage
{
    private static final long serialVersionUID = 1198546600602532976L;

    private final String uploadPath;
    private final String explanation;
    private final Set<FileAttribute> requested;
    private Collection<FileAttributes> deletedFiles;

    public PnfsCancelUpload(Subject subject, Restriction restriction,
            FsPath uploadPath, FsPath path, Set<FileAttribute> requested, String explanation)
    {
        setSubject(subject);
        setRestriction(restriction);
        setPnfsPath(path.toString());
        setReplyRequired(true);
        this.uploadPath = uploadPath.toString();
        this.explanation = requireNonNull(explanation);
        this.requested = requireNonNull(requested);
    }

    public FsPath getPath()
    {
        return FsPath.create(getPnfsPath());
    }

    public FsPath getUploadPath()
    {
        return FsPath.create(uploadPath);
    }

    public String getExplanation()
    {
        return explanation;
    }

    public Set<FileAttribute> getRequestedAttributes()
    {
        return requested;
    }

    public void setDeletedFiles(Collection<FileAttributes> files)
    {
        deletedFiles = requireNonNull(files);
    }

    public Collection<FileAttributes> getDeletedFiles()
    {
        return deletedFiles;
    }
}
