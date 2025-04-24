/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
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
package org.dcache.util.list;

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;

import java.util.Set;
import javax.security.auth.Subject;

import org.dcache.auth.attributes.Restriction;
import org.dcache.namespace.FileAttribute;

/**
 * Interface for components that can providing a vurtual directory listing.
 * <p>
 * Should be merged with PnfsHandler or the new client lib for PnfsManager.
 * <p>
 * All operations have a Subject parameter. If a Subject is supplied, then permission checks are
 * applied and PermissionDeniedCacheException is thrown if the Subject does not have permissions to
 * perform the operation. If a null Subject is supplied, then an implementation specific default is
 * applied.
 */
public interface VirtualDirectoryListSource {


    /**
     * Lists the content of a virtual directory. The content is returned as a
     * directory stream containing files with a given label (path).
     *
     * @param subject     The Subject of the user performing the
     *                    operation; may be null
     * @param restriction a login attribute; may be zero or more
     * @param path        Path to virtual directory to list (which is the label value of a streamed files)
     * @param range       The range of entries to return; may be null
     * @param attrs       The file attributes to query for each entry
     * @return A DirectoryStream of the entries in the directory
     */
    DirectoryStream listVirtualDirectory(Subject subject, Restriction restriction, FsPath path,
                                         Range<Integer> range,
                                         Set<FileAttribute> attrs)
            throws InterruptedException, CacheException;


}
