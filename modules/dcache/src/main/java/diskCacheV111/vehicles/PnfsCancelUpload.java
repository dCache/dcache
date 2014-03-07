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

import diskCacheV111.util.FsPath;

/**
 * Revoke a temporary upload path.
 *
 * This message operates on a temporary upload path generated with
 * PnfsCreateUploadPath. The temporary upload path will be deleted
 * and the path will no longer be available for writing.
 */
public class PnfsCancelUpload extends PnfsMessage
{
    private final String uploadPath;

    public PnfsCancelUpload(Subject subject, FsPath uploadPath, FsPath path)
    {
        setSubject(subject);
        setPnfsPath(path.toString());
        setReplyRequired(true);
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
}
