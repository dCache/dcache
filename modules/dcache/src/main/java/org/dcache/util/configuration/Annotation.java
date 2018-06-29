/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2011 - 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.util.configuration;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *  This enum represents a property key annotation.  Each annotation has
 *  an associated label that is present as a comma-separated list within
 *  parentheses.
 */
public enum Annotation
{
    FORBIDDEN("forbidden"),
    OBSOLETE("obsolete"),
    ONE_OF("one-of", true),
    DEPRECATED("deprecated"),
    NOT_FOR_SERVICES("not-for-services"),
    IMMUTABLE("immutable"),
    ANY_OF("any-of", true),
    PREFIX("prefix");

    private static final Map<String,Annotation> ANNOTATION_LABELS =
            new HashMap<>();

    private final String _label;
    private final boolean _isParameterRequired;

    static {
        for( Annotation annotation : Annotation.values()) {
            ANNOTATION_LABELS.put(annotation._label, annotation);
        }
    }

    public static Annotation forLabel(String label)
    {
        checkArgument(ANNOTATION_LABELS.containsKey(label),
                "Unknown annotation: " + label);
        return ANNOTATION_LABELS.get(label);
    }

    Annotation(String label)
    {
        this(label, false);
    }

    Annotation(String label, boolean isParameterRequired)
    {
        _label = label;
        _isParameterRequired = isParameterRequired;
    }

    public boolean isParameterRequired()
    {
        return _isParameterRequired;
    }
}
