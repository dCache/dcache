/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class AppendOnlySetTest
{
    Set<Item> inner;
    Set<Item> appendOnly;

    private class Item
    {
    }

    @Before
    public void setup()
    {
        inner = new HashSet();
        appendOnly = new AppendOnlySet(inner);
    }

    @Test
    public void shouldAllowAdd()
    {
        Item item = new Item();

        appendOnly.add(item);

        assertThat(appendOnly, hasItem(item));
        assertThat(inner, hasItem(item));
    }

    @Test
    public void shouldAllowAddAll()
    {
        Item item1 = new Item();
        Item item2 = new Item();
        List<Item> items = Arrays.asList(item1, item2);

        appendOnly.addAll(items);

        assertThat(appendOnly, hasItems(item1, item2));
        assertThat(inner, hasItems(item1, item2));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shoudNotAllowRemoveItem()
    {
        Item item = new Item();
        appendOnly.add(item);

        appendOnly.remove(item);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shoudNotAllowRemoveItemThroughIterator()
    {
        Item item = new Item();
        appendOnly.add(item);

        Iterator<Item> i = appendOnly.iterator();
        i.next();

        i.remove();
    }
}
