/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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

import java.util.Collections;
import java.util.Set;

import static org.dcache.namespace.FileAttribute.LABELS;
import static org.junit.Assert.*;

public class FileLabelsTest
{
    FileAttributes fileAttributes;

    @Test
    public void shouldNotHaveLabelsInitially()
    {
        given(fileAttributes());
        assertFalse(fileAttributes.hasLabel("cat"));
        assertFalse(fileAttributes.isDefined(LABELS));
    }

    @Test
    public void shouldAllowFetchingLabelsInitially()
    {
        given(fileAttributes());
        fileAttributes.getLabels();
    }

    @Test
    public void shouldAcceptLabels()
    {
        given(fileAttributes().withLabel("dog"));
        assertTrue(fileAttributes.isDefined(LABELS));
        assertTrue(fileAttributes.hasLabel("dog"));
        assertTrue(fileAttributes.getLabels().contains("dog"));
    }


    @Test
    public void checkSetLabels()
    {
        Set<String> labels = Set.of("dog", "cat", "yellow");
        given(fileAttributes().withLabels(labels));
        assertEquals(labels, fileAttributes.getLabels());
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
        public FileAttributesBuilder withLabel(String name)
        {
            attributes.setLabels(Collections.singleton(name));
            return this;
        }


        public FileAttributesBuilder withLabels(Set<String> names)
        {
            attributes.setLabels(names);
            return this;
        }

        public FileAttributes build()
        {
            return attributes;
        }
    }
}