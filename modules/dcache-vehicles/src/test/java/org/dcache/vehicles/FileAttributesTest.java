/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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

import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static org.dcache.namespace.FileAttribute.XATTR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.*;

public class FileAttributesTest
{
    FileAttributes fileAttributes;

    @Test
    public void shouldNotHaveXattrInitially()
    {
        given(fileAttributes());

        assertFalse(fileAttributes.hasXattr("my-xattr"));
        assertFalse(fileAttributes.isDefined(XATTR));
    }

    @Test(expected=IllegalStateException.class)
    public void shouldDisallowFetchingXattrInitially()
    {
        given(fileAttributes());

        fileAttributes.getXattrs();
    }

    @Test
    public void shouldAcceptXattrMap()
    {
        given(fileAttributes());

        fileAttributes.setXattrs(Map.of("my-xattr", "my-value"));

        assertTrue(fileAttributes.isDefined(XATTR));
        assertTrue(fileAttributes.hasXattr("my-xattr"));
        assertThat(fileAttributes.getXattrs(), hasEntry("my-xattr", "my-value"));
    }

    @Test
    public void shouldUpdateEmptyXattr()
    {
        given(fileAttributes());

        Optional<String> oldValue = fileAttributes.updateXattr("my-xattr", "my-value");

        assertFalse(oldValue.isPresent());
        assertTrue(fileAttributes.isDefined(XATTR));
        assertTrue(fileAttributes.hasXattr("my-xattr"));
        assertThat(fileAttributes.getXattrs(), hasEntry("my-xattr", "my-value"));
    }

    @Test
    public void shouldUpdateExistingXattr()
    {
        given(fileAttributes().withXattr("my-xattr", "my-old-value"));

        Optional<String> oldValue = fileAttributes.updateXattr("my-xattr", "my-value");

        assertThat(oldValue.get(), equalTo("my-old-value"));
        assertTrue(fileAttributes.isDefined(XATTR));
        assertTrue(fileAttributes.hasXattr("my-xattr"));
        assertThat(fileAttributes.getXattrs(), hasEntry("my-xattr", "my-value"));
    }

    private void given(FileAttributesBuilder builder)
    {
        fileAttributes = builder.build();
    }

    private FileAttributesBuilder fileAttributes()
    {
        return new FileAttributesBuilder();
    }

    private static class FileAttributesBuilder
    {
        FileAttributes attributes = new FileAttributes();

        public FileAttributesBuilder withXattr(String name, String value)
        {
            attributes.updateXattr(name, value);
            return this;
        }

        public FileAttributes build()
        {
            return attributes;
        }
    }
}
