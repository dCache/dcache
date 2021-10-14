/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2016 Deutsches Elektronen-Synchrotron
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

import static org.dcache.namespace.FileAttribute.PNFSID;

import diskCacheV111.util.FsPath;
import java.util.Collections;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

/**
 * Commit an upload path to its final name.
 * <p>
 * The file written to the temporary upload path is moved to its final location. The upload path
 * will no longer be writable after this message.
 */
public class PnfsCommitUpload extends PnfsMessage {

    private static final long serialVersionUID = -2574528537801095072L;

    private final String uploadPath;
    private final Set<FileAttribute> requestedAttributes;
    private final Set<CreateOption> options;
    private FileAttributes fileAttributes;

    public PnfsCommitUpload(Subject subject,
          Restriction restriction,
          FsPath uploadPath,
          FsPath path,
          Set<CreateOption> options,
          Set<FileAttribute> requestedAttributes) {
        setSubject(subject);
        setRestriction(restriction);
        setPnfsPath(path.toString());
        setReplyRequired(true);
        this.options = options;
        this.requestedAttributes = requestedAttributes;
        this.uploadPath = uploadPath.toString();
    }

    public FsPath getPath() {
        return FsPath.create(getPnfsPath());
    }

    public FsPath getUploadPath() {
        return FsPath.create(uploadPath);
    }

    public Set<CreateOption> getOptions() {
        return Collections.unmodifiableSet(options);
    }

    public Set<FileAttribute> getRequestedAttributes() {
        return requestedAttributes;
    }

    public FileAttributes getFileAttributes() {
        return fileAttributes;
    }

    public void setFileAttributes(FileAttributes fileAttributes) {
        if (fileAttributes.isDefined(PNFSID)) {
            setPnfsId(fileAttributes.getPnfsId());
        }
        this.fileAttributes = fileAttributes;
    }
}
