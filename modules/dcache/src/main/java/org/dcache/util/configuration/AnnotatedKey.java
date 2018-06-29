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

import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A class for parsing and storing a set of annotations associated with
 * some specific property declaration's key in addition to a potential
 * custom error message.
 *
 * Annotations take the form of a comma-separated list of keywords
 * within parentheses that immediately precede the property name;
 *
 * If a property is annotated as forbidden then the property value is taken
 * as a custom error message to report.  If the value is empty then a default
 * error message is used instead.
 */
public class AnnotatedKey
{
    private static final String RE_ATTRIBUTE = "[^),]+";
    private static final String RE_SEPARATOR = ",";
    private static final String RE_ANNOTATION_DECLARATION =
        "(\\((" + RE_ATTRIBUTE + "(?:" + RE_SEPARATOR + RE_ATTRIBUTE + ")*)\\))";
    private static final String RE_KEY_DECLARATION =
        RE_ANNOTATION_DECLARATION + "(.*)";

    private static final Pattern PATTERN_KEY_DECLARATION = Pattern.compile(RE_KEY_DECLARATION);
    private static final Pattern PATTERN_SEPARATOR = Pattern.compile(RE_SEPARATOR);

    private static final Set<Annotation> FORBIDDEN_OBSOLETE_DEPRECATED =
        EnumSet.of(Annotation.FORBIDDEN, Annotation.OBSOLETE, Annotation.DEPRECATED);

    private static final Set<Annotation> FORBIDDEN_OBSOLETE =
        EnumSet.of(Annotation.FORBIDDEN, Annotation.OBSOLETE);

    private final String _name;
    private final String _annotationDeclaration;
    private final Map<Annotation,String> _annotations =
            new EnumMap<>(Annotation.class);
    private final String _error;

    public AnnotatedKey(Object propertyKey, Object propertyValue)
    {
        String key = propertyKey.toString();
        Matcher m = PATTERN_KEY_DECLARATION.matcher(key);
        if(m.matches()) {
            _annotationDeclaration = m.group(1);

            for(String annotation : PATTERN_SEPARATOR.split(m.group(2))) {
                addAnnotation(annotation);
            }

            _name = m.group(3);

            if(countDeclaredAnnotationsFrom(FORBIDDEN_OBSOLETE_DEPRECATED) > 1) {
                throw new IllegalArgumentException("At most one of forbidden, obsolete " +
                        "and deprecated may be specified.");
            }
        } else {
            _annotationDeclaration = "";
            _name = key;
        }

        _error = hasAnyOf(FORBIDDEN_OBSOLETE) ? propertyValue.toString() : "";
    }

    /**
     * Process an individual attribute declaration.  An annotation has
     * one or more attributes.  Each attribute has the form:
     * <pre>&lt;label>['?'&lt;parameter>]</pre>
     */
    private void addAnnotation(String declaration)
    {
        int idx = declaration.indexOf('?');
        String label = (idx != -1) ? declaration.substring(0, idx) :
                declaration;
        Annotation annotation = Annotation.forLabel(label);

        checkArgument(!annotation.isParameterRequired() || idx != -1,
                "Annotation " + label + " declared without parameter");
        checkArgument(annotation.isParameterRequired() || idx == -1,
                "Annotation " + label + " declared with parameter");

        if(annotation.isParameterRequired()) {
            String parameter = declaration.substring(idx+1,
                    declaration.length());
            _annotations.put(annotation, parameter);
        } else {
            _annotations.put(annotation, null);
        }
    }

    private int countDeclaredAnnotationsFrom(Set<Annotation> items) {
        Collection<Annotation> a = EnumSet.copyOf(items);
        a.retainAll(_annotations.keySet());
        return a.size();
    }

    public boolean hasAnnotation(Annotation annotation) {
        return _annotations.keySet().contains(annotation);
    }

    public final boolean hasAnyOf(Set<Annotation> annotations) {
        return countDeclaredAnnotationsFrom(annotations) > 0;
    }

    public boolean hasAnnotations() {
        return !_annotations.isEmpty();
    }

    public String getAnnotationDeclaration() {
        return _annotationDeclaration;
    }

    public String getPropertyName() {
        return _name;
    }

    public String getError() {
        return _error;
    }

    public boolean hasError() {
        return !_error.isEmpty();
    }

    public String getParameter(Annotation annotation) {
        String parameter = _annotations.get(annotation);

        if(parameter == null) {
            throw new IllegalArgumentException("No such annotation or " +
                    "annotation given without parameter: " + annotation);
        }

        return parameter;
    }
}
