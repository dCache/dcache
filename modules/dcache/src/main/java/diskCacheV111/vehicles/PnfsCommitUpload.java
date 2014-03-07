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

import diskCacheV111.util.FsPath;

import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

/**
 * Commit an upload path to its final name.
 *
 * The file written to the temporary upload path is moved to its final
 * location. The upload path will no longer be writable after this message.
 */
public class PnfsCommitUpload extends PnfsMessage
{
    private final String uploadPath;
    private final Set<FileAttribute> requestedAttributes;
    private final Set<CreateOption> options;
    private FileAttributes fileAttributes;

    public PnfsCommitUpload(Subject subject,
                            FsPath uploadPath,
                            FsPath path,
                            Set<CreateOption> options,
                            Set<FileAttribute> requestedAttributes)
    {
        setSubject(subject);
        setPnfsPath(path.toString());
        setReplyRequired(true);
        this.options = options;
        this.requestedAttributes = requestedAttributes;
        this.uploadPath = uploadPath.toString();
    }

    public FsPath getPath()
    {
        return new FsPath(getPnfsPath());
    }

    public FsPath getUploadPath()
    {
        return new FsPath(uploadPath);
    }

    public Set<CreateOption> getOptions()
    {
        return Collections.unmodifiableSet(options);
    }

    public Set<FileAttribute> getRequestedAttributes()
    {
        return requestedAttributes;
    }

    public FileAttributes getFileAttributes()
    {
        return fileAttributes;
    }

    public void setFileAttributes(FileAttributes fileAttributes)
    {
        this.fileAttributes = fileAttributes;
    }
}
