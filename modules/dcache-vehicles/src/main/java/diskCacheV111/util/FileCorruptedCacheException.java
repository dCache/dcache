/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2007-2018 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.util;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.dcache.util.Checksum;

/**
 * Signals that the file size or checksum of a file does not match the expected
 * checksum or file size, or that a file or replica is otherwise corrupted.
 *
 * Note that expected and actual file size or checksum stored in the exception
 * are not preserved by cells message passing.
 */
public class FileCorruptedCacheException extends CacheException
{
    private static final long serialVersionUID = 6022529795888425409L;

    private final Set<Checksum> _expectedChecksums;
    private final Set<Checksum> _actualChecksums;
    private final Long _expectedSize;
    private final Long _actualSize;

    public FileCorruptedCacheException(String message)
    {
        super(FILE_CORRUPTED, message);
        _expectedChecksums = null;
        _actualChecksums = null;
        _expectedSize = null;
        _actualSize = null;
    }

    public FileCorruptedCacheException(String message, Throwable cause)
    {
        super(FILE_CORRUPTED, message, cause);
        _expectedChecksums = null;
        _actualChecksums = null;
        _expectedSize = null;
        _actualSize = null;
    }

    public FileCorruptedCacheException(Checksum expectedChecksum, Checksum actualChecksum)
    {
        this(Collections.singleton(expectedChecksum), Collections.singleton(actualChecksum));
    }

    public FileCorruptedCacheException(Set<Checksum> expectedChecksums, Set<Checksum> actualChecksums)
    {
        super(FILE_CORRUPTED, "Checksum mismatch (expected=" + expectedChecksums + ", actual=" + actualChecksums + ')');
        _expectedChecksums = expectedChecksums;
        _actualChecksums = actualChecksums;
        _expectedSize = null;
        _actualSize = null;
    }

    public FileCorruptedCacheException(long expectedSize, long actualSize)
    {
        super(FILE_CORRUPTED, "File size mismatch (expected=" + expectedSize + ", actual=" + actualSize + ')');
        _expectedChecksums = null;
        _actualChecksums = null;
        _expectedSize = expectedSize;
        _actualSize = actualSize;
    }

    public Optional<Set<Checksum>> getExpectedChecksums()
    {
        return Optional.ofNullable(_expectedChecksums);
    }

    public Optional<Set<Checksum>> getActualChecksums()
    {
        return Optional.ofNullable(_actualChecksums);
    }

    public Optional<Long> getExpectedSize()
    {
        return Optional.ofNullable(_expectedSize);
    }

    public Optional<Long> getActualSize()
    {
        return Optional.ofNullable(_actualSize);
    }
}
