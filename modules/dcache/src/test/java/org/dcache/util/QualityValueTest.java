/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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

import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class QualityValueTest
{
    @Test
    public void shouldParseWithSpaceQValue()
    {
        QualityValue qvalue = QualityValue.of("audio/*; q=0.2");
        assertThat(qvalue.value(), is(equalTo("audio/*")));
        assertThat(qvalue.quality(), is(equalTo(0.2)));
    }

    @Test
    public void shouldParseWithOneQValue()
    {
        QualityValue qvalue = QualityValue.of("audio/*;q=1");
        assertThat(qvalue.value(), is(equalTo("audio/*")));
        assertThat(qvalue.quality(), is(equalTo(1.0)));
    }

    @Test
    public void shouldParseWithOnePointQValue()
    {
        QualityValue qvalue = QualityValue.of("audio/*;q=1.");
        assertThat(qvalue.value(), is(equalTo("audio/*")));
        assertThat(qvalue.quality(), is(equalTo(1.0)));
    }

    @Test
    public void shouldParseWithOnePointZeroQValue()
    {
        QualityValue qvalue = QualityValue.of("audio/*;q=1.0");
        assertThat(qvalue.value(), is(equalTo("audio/*")));
        assertThat(qvalue.quality(), is(equalTo(1.0)));
    }

    @Test
    public void shouldParseWithOnePointZeroZeroQValue()
    {
        QualityValue qvalue = QualityValue.of("audio/*;q=1.00");
        assertThat(qvalue.value(), is(equalTo("audio/*")));
        assertThat(qvalue.quality(), is(equalTo(1.0)));
    }

    @Test
    public void shouldParseWithOnePointZeroZeroZeroQValue()
    {
        QualityValue qvalue = QualityValue.of("audio/*;q=1.000");
        assertThat(qvalue.value(), is(equalTo("audio/*")));
        assertThat(qvalue.quality(), is(equalTo(1.0)));
    }

    @Test
    public void shouldParseWithoutQValue()
    {
        QualityValue qvalue = QualityValue.of("audio/basic");

        assertThat(qvalue.value(), is(equalTo("audio/basic")));
        assertThat(qvalue.quality(), is(equalTo(1.0)));
    }

    @Test
    public void shouldOrderByQuality()
    {
        QualityValue qvalue1 = QualityValue.of("value1;q=1");
        QualityValue qvalue2 = QualityValue.of("value2;q=0.5");
        List<QualityValue> expected = Arrays.asList(qvalue1, qvalue2);
        List<QualityValue> actual = Arrays.asList(qvalue1, qvalue2);
        actual.sort(Comparator.naturalOrder());
        assertThat(actual, is(equalTo(expected)));
    }

    @Test
    public void shouldOrderByQualityWhenReversed()
    {
        QualityValue qvalue1 = QualityValue.of("value1;q=0.5");
        QualityValue qvalue2 = QualityValue.of("value2;q=1");
        List<QualityValue> expected = Arrays.asList(qvalue2, qvalue1);
        List<QualityValue> actual = Arrays.asList(qvalue1, qvalue2);
        actual.sort(Comparator.naturalOrder());
        assertThat(actual, is(equalTo(expected)));
    }
}
