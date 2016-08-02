/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2007-2013 Deutsches Elektronen-Synchrotron
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

import com.google.common.base.Optional;

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

    private final Optional<Set<Checksum>> _expectedChecksums;
    private final Optional<Set<Checksum>> _actualChecksums;
    private final Optional<Long> _expectedSize;
    private final Optional<Long> _actualSize;

    public FileCorruptedCacheException(String message)
    {
        super(FILE_CORRUPTED, message);
        _expectedChecksums = Optional.absent();
        _actualChecksums = Optional.absent();
        _expectedSize = Optional.absent();
        _actualSize = Optional.absent();
    }

    public FileCorruptedCacheException(String message, Throwable cause)
    {
        super(FILE_CORRUPTED, message, cause);
        _expectedChecksums = Optional.absent();
        _actualChecksums = Optional.absent();
        _expectedSize = Optional.absent();
        _actualSize = Optional.absent();
    }

    public FileCorruptedCacheException(Set<Checksum> expectedChecksums, Set<Checksum> actualChecksums)
    {
        super(FILE_CORRUPTED, "Checksum mismatch (expected=" + expectedChecksums + ", actual=" + actualChecksums + ')');
        _expectedChecksums = Optional.of(expectedChecksums);
        _actualChecksums = Optional.of(actualChecksums);
        _expectedSize = Optional.absent();
        _actualSize = Optional.absent();
    }

    public FileCorruptedCacheException(long expectedSize, long actualSize)
    {
        super(FILE_CORRUPTED, "File size mismatch (expected=" + expectedSize + ", actual=" + actualSize + ')');
        _expectedChecksums = Optional.absent();
        _actualChecksums = Optional.absent();
        _expectedSize = Optional.of(expectedSize);
        _actualSize = Optional.of(actualSize);
    }

    public Optional<Set<Checksum>> getExpectedChecksums()
    {
        return _expectedChecksums;
    }

    public Optional<Set<Checksum>> getActualChecksums()
    {
        return _actualChecksums;
    }

    public Optional<Long> getExpectedSize()
    {
        return _expectedSize;
    }

    public Optional<Long> getActualSize()
    {
        return _actualSize;
    }
}
