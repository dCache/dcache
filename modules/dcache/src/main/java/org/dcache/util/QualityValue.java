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

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A quality value (qvalue) string as defined in RFC 7231.  A ranking of
 * possible values with an associated desirability ("quality") between 0.0 and
 * 1.0, with 1.0 indicating most desired, 0.001 indicating least acceptable,
 * and 0 indicating not acceptable.
 */
public class QualityValue<T> implements Comparable<QualityValue>
{
    private static final Pattern QVALUE_PATTERN = Pattern.compile(";[ \\t]*[Qq]=((?:1(?:\\.0{0,3})?)|(?:0(?:.[0-9]{0,3})?))$");

    private final String rawValue;
    private final T convertedValue;
    private final double quality;

    /**
     * Provide a simple QualityValue without any conversion.
     * @param qvalue a qvalue
     * @return The parsed qvalue.
     */
    public static QualityValue<String> of(String qvalue)
    {
        return new QualityValue(qvalue, v -> v);
    }

    /**
     * A qvalue String.  The string may contain a q-value (e.g.,
     * {@literal foo;q=0.5}) or have no q-value (e.g., {@literal foo}).
     * @param value a qvalue String
     * @param conversion a method that converts the qvalue to the desired type.
     */
    public QualityValue(String value, Function<String,T> conversion)
    {
        Matcher m = QVALUE_PATTERN.matcher(value);
        if (m.find()) {
            rawValue = value.substring(0, m.start());
            quality = Double.parseDouble(m.group(1));
        } else {
            rawValue = value;
            quality = 1;
        }
        this.convertedValue = conversion.apply(rawValue);
    }

    private QualityValue(String rawValue, T value, double quality)
    {
        this.rawValue = rawValue;
        this.convertedValue = value;
        this.quality = quality;
    }

    /**
     * Provide a new QualityValue with the same quality but with
     * the value mapped to a different type.
     * @param <U> The new type of the value
     * @param conversion the method to convert to the new type
     * @return The mapped QualityValue.
     */
    public <U> QualityValue<U> mapWith(Function<String,U> conversion)
    {
        return new QualityValue(rawValue, conversion.apply(rawValue), quality);
    }

    /**
     * The desirability (or quality) of this qvalue.
     * @return a value between 0.0 (inclusive) and 1.0 (inclusive).
     */
    public double quality()
    {
        return quality;
    }

    /**
     * The qvalue text without the optional {@code ;q=...} suffix.
     * @return The value associated with this qvalue string.
     */
    public T value()
    {
        return convertedValue;
    }

    @Override
    public int compareTo(QualityValue o)
    {
        return Double.compare(o.quality, quality);
    }
}
