/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020-2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.vehicles;

import static java.util.Collections.singleton;
import static org.dcache.namespace.FileAttribute.XATTR;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.util.ChecksumType.ADLER32;
import static org.dcache.util.ChecksumType.MD5_TYPE;
import static org.dcache.util.FileAttributesBuilder.fileAttributes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.dcache.util.Checksum;
import org.dcache.util.FileAttributesBuilder;
import org.junit.Test;

public class FileAttributesTest {

    FileAttributes fileAttributes;

    @Test
    public void shouldNotHaveXattrInitially() {
        given(fileAttributes());

        assertFalse(fileAttributes.hasXattr("my-xattr"));
        assertFalse(fileAttributes.isDefined(XATTR));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldDisallowFetchingXattrInitially() {
        given(fileAttributes());

        fileAttributes.getXattrs();
    }

    @Test
    public void shouldAcceptXattrMap() {
        given(fileAttributes());

        fileAttributes.setXattrs(Map.of("my-xattr", "my-value"));

        assertTrue(fileAttributes.isDefined(XATTR));
        assertTrue(fileAttributes.hasXattr("my-xattr"));
        assertThat(fileAttributes.getXattrs(), hasEntry("my-xattr", "my-value"));
    }

    @Test
    public void shouldUpdateEmptyXattr() {
        given(fileAttributes());

        Optional<String> oldValue = fileAttributes.updateXattr("my-xattr", "my-value");

        assertFalse(oldValue.isPresent());
        assertTrue(fileAttributes.isDefined(XATTR));
        assertTrue(fileAttributes.hasXattr("my-xattr"));
        assertThat(fileAttributes.getXattrs(), hasEntry("my-xattr", "my-value"));
    }

    @Test
    public void shouldUpdateExistingXattr() {
        given(fileAttributes().withXattr("my-xattr", "my-old-value"));

        Optional<String> oldValue = fileAttributes.updateXattr("my-xattr", "my-value");

        assertThat(oldValue.get(), equalTo("my-old-value"));
        assertTrue(fileAttributes.isDefined(XATTR));
        assertTrue(fileAttributes.hasXattr("my-xattr"));
        assertThat(fileAttributes.getXattrs(), hasEntry("my-xattr", "my-value"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNPEWhenAddChecksumWithNull() {
        given(fileAttributes());

        fileAttributes.addChecksums(null);
    }

    @Test
    public void shouldIgnoreEmptyAddChecksumWithoutExistingChecksum() {
        given(fileAttributes());

        fileAttributes.addChecksums(new HashSet<>());

        assertFalse(fileAttributes.isDefined(CHECKSUM));
    }

    @Test
    public void shouldIgnoreEmptyAddChecksumWithExistingChecksum() {
        Checksum existingChecksum = new Checksum(ADLER32, "321b0621");
        given(fileAttributes().withChecksum(existingChecksum));

        fileAttributes.addChecksums(new HashSet<>());

        assertTrue(fileAttributes.isDefined(CHECKSUM));
        assertThat(fileAttributes.getChecksums(), contains(existingChecksum));
    }

    @Test
    public void shouldAcceptNewChecksumWhenAddChecksumWithoutExistingChecksum() {
        given(fileAttributes());

        var newChecksum = new Checksum(MD5_TYPE, "d41d8cd98f00b204e9800998ecf8427e");
        fileAttributes.addChecksums(singleton(newChecksum));

        assertTrue(fileAttributes.isDefined(CHECKSUM));
        assertThat(fileAttributes.getChecksums(), contains(newChecksum));
    }

    @Test
    public void shouldAcceptNewChecksumWhenAddChecksumWithExistingChecksum() {
        Checksum existingChecksum = new Checksum(ADLER32, "321b0621");
        given(fileAttributes().withChecksum(existingChecksum));

        Checksum extraChecksum = new Checksum(MD5_TYPE, "d41d8cd98f00b204e9800998ecf8427e");
        fileAttributes.addChecksums(singleton(extraChecksum));

        assertTrue(fileAttributes.isDefined(CHECKSUM));
        assertThat(fileAttributes.getChecksums(), containsInAnyOrder(existingChecksum, extraChecksum));
    }

    private void given(FileAttributesBuilder builder) {
        fileAttributes = builder.build();
    }
}
