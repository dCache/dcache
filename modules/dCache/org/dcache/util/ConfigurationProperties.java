package org.dcache.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import dmg.util.Formats;
import dmg.util.PropertiesBackedReplaceable;

/**
 * The ConfigurationProperties class represents a set of dCache
 * configuration properties.
 * <p>
 * Repeated declaration of the same property is considered an error
 * and will cause loading of configuration files to fail.
 * <p>
 * Properties may have zero or more annotations.  These annotations
 * are represented as a comma-separated list of annotation-labels
 * inside parentheses immediately before the property key. Valid
 * annotation labels are "deprecated", "obsolete", "forbidden"
 * and "not-for-services".  A property may have, at most, one annotation
 * from the set {deprecated, obsolete, forbidden}.
 * <p>
 * Annotations have the following semantics:
 * <ul>
 * <li><i>deprecated</i> indicates that a property is supported but that a
 * future version of dCache will likely remove that support.
 * <li><i>obsolete</i> indicates that a property is no longer supported and
 * that dCache will always behaves correctly without supporting this
 * property.
 * <li><i>forbidden</i> indicates that a property is no longer supported and
 * dCache does not always behave correctly without further configuration or
 * that support for some feature has been removed.
 * <li><i>not-for-services</i> indicates that a property has no effect if
 * the property is assigned a value within a service context.
 * </ul>
 * <p>
 * The intended behaviour of dCache when encountering sysadmin-supplied
 * property assignment of some annotated property is dependent on the
 * annotation(s).  If the property is annotated as deprecated and obsolete
 * then a warning is emitted and dCache continues to start up. If the user
 * assigns a value to a forbidden properties then dCache will refuse to start.
 * <p>
 * Annotation of a property only affects subsequent declarations.  It does not
 * affect any previous declarations of this property, nor does it generate any
 * errors when such properties are referenced in any way.
 * <p>
 * The following provides examples of valid annotated declarations:
 * <pre>
 *   (obsolete)dcache.property1 = some value
 *   (forbidden)dcache.property2 =
 *   (deprecated,no-for-services)dcache.property3 = default-value
 * </pre>
 *
 * @see java.util.Properties
 */
public class ConfigurationProperties
    extends Properties
{
    private static final long serialVersionUID = -5684848160314570455L;

    private static final String STORAGE_PREFIX_ANNOTATIONS = "<A>";
    private static final String STORAGE_PREFIX_ERROR_MSG = "<E>";

    private static final Pattern SINGLE_PROPERTY_EXPANSION = Pattern.compile("^\\$\\{([^}]+)\\}");

    private static final EnumSet<Annotation> OBSOLETE_FORBIDDEN =
        EnumSet.of(Annotation.OBSOLETE, Annotation.FORBIDDEN);

    private final static Logger _log =
        LoggerFactory.getLogger(ConfigurationProperties.class);

    private final PropertiesBackedReplaceable _replaceable =
        new PropertiesBackedReplaceable(this);

    private boolean _loading = false;
    private boolean _isService = false;

    public ConfigurationProperties()
    {
        super();
    }

    public ConfigurationProperties(Properties defaults)
    {
        super(defaults);
    }

    public void setIsService(boolean isService)
    {
        _isService = isService;
    }

    public AnnotatedKey getAnnotatedKey(String name) {
        AnnotatedKey key;

        String annotations = getProperty(STORAGE_PREFIX_ANNOTATIONS + name);

        if(annotations != null) {
            String error = getProperty(STORAGE_PREFIX_ERROR_MSG + name);
            key = new AnnotatedKey(name, annotations, error);
        } else {
            key = new AnnotatedKey(name, "");
        }

        return key;
    }

    /**
     * Returns whether a name is scoped.
     *
     * A scoped name begins with the name of the scope followed by the
     * scoping operator, a forward slash.
     */
    public static boolean isScoped(String name)
    {
        return name.indexOf('/') > -1;
    }

    /**
     * @throws IllegalArgumentException during loading if a property
     * is defined multiple times.
     */
    @Override
    public synchronized void load(Reader reader) throws IOException
    {
        _loading = true;
        try {
            super.load(reader);
        } finally {
            _loading = false;
        }
    }

    /**
     * @throws IllegalArgumentException during loading if a property
     * is defined multiple times.
     */
    @Override
    public synchronized void load(InputStream in) throws IOException
    {
        _loading = true;
        try {
            super.load(in);
        } finally {
            _loading = false;
        }
    }

    /**
     * @throws IllegalArgumentException during loading if a property
     * is defined multiple times or the annotations are inappropriate.
     */
    @Override
    public synchronized void loadFromXML(InputStream in)
        throws IOException, InvalidPropertiesFormatException
    {
        _loading = true;
        try {
            super.loadFromXML(in);
        } finally {
            _loading = false;
        }
    }

    /**
     * Loads a Java properties file.
     */
    public void loadFile(File file)
        throws IOException
    {
        Reader in = new FileReader(file);
        try {
            load(in);
        } finally {
            in.close();
        }
    }

    /**
     * @throws IllegalArgumentException during loading if key is
     * already defined.
     */
    @Override
    public synchronized Object put(Object rawKey, Object value)
    {
        checkNotNull(rawKey, "A propery key must not be null");
        checkNotNull(value, "A propery value must not be null");

        AnnotatedKey key = new AnnotatedKey(rawKey, value);
        String name = key.getPropertyName();
        checkArgument(!_loading || !containsKey(name), "%s is already defined", name);

        checkIsAllowedKey(key);

        if(key.hasAnnotations()) {
            store(key);
        }

        return key.hasAnyOf(OBSOLETE_FORBIDDEN) ? null : super.put(name, value);
    }


    private void checkIsAllowedKey(AnnotatedKey key)
    {
        String name = key.getPropertyName();

        AnnotatedKey existingKey = getAnnotatedKey(name);

        if(existingKey.hasAnnotations() && key.hasAnnotations()) {
            throw new IllegalArgumentException("Property " + name + ": " +
                    "remove \"" + key.getAnnotationDeclaration() + "\"; " +
                    "annotated assignments are not allowed");
        }

        if(existingKey.hasAnnotation(Annotation.FORBIDDEN)) {
            throw new IllegalArgumentException(forbiddenErrorMessageFor(existingKey));
        }

        if(existingKey.hasAnnotation(Annotation.OBSOLETE)) {
            _log.warn(obsoleteErrorMessageFor(existingKey));
        }

        if(existingKey.hasAnnotation(Annotation.DEPRECATED)) {
            _log.warn( "Property {}: {}; support for {} will be removed in the future",
                        new Object[] {name, deprecatedWarningInstructionsFor(name), name});
        }

        if(_isService && existingKey.hasAnnotation(Annotation.NOT_FOR_SERVICES)) {
            _log.warn( "Property {}: consider moving to a domain scope; it has no effect here",
                        name);
        }
    }

    private String deprecatedWarningInstructionsFor(String propertyName)
    {
        String synonym = findSynonymOf(propertyName);

        if(synonym != null) {
            return "use \"" + synonym + "\" instead";
        } else {
            return "please review configuration";
        }
    }

    /**
     * Define the binary relationship property A hasSynonym property B
     * as true iff either:
     * <ul>
     * <li>Property A's value is a simple reference to property B (e.g.
     * <tt>property.A = ${property.B}</tt>).
     * <li>If there exists precisely one property with a simple reference to
     * property A; e.g.
     * <pre>
     *     property.B = ${property.A}
     *     property.A = some default value
     * </pre>
     * <p>
     * This method returns the name of a property that is the subject of
     * hasSynonym relationship (property B) with the supplied property
     * (as property A), or null if no such property exists.
     */
    private String findSynonymOf(String propertyName)
    {
        String propertyValue = getProperty(propertyName);
        Matcher m = SINGLE_PROPERTY_EXPANSION.matcher(propertyValue);
        if( m.matches()) {
            return m.group(1);
        }

        String synonym = null;
        String simpleReference = "${" + propertyName + "}";

        for(Map.Entry<Object,Object> entry : entrySet()) {
            String value = entry.getValue().toString();
            if( value.equals(simpleReference)) {
                if( synonym != null) {
                    return null;
                }

                synonym = entry.getKey().toString();
            }
        }

        return synonym;
    }

    private String forbiddenErrorMessageFor(AnnotatedKey key)
    {
        String customError = key.getError();

        String suffix = customError.isEmpty() ? "this property no longer affects dCache" :
            customError;

        return "Property " + key.getPropertyName() + ": may not be adjusted; " + suffix;
    }

    private String obsoleteErrorMessageFor(AnnotatedKey key)
    {
        String customError = key.getError();

        String suffix = customError.isEmpty() ? "it has no effect" : customError;

        return "Property " + key.getPropertyName() + ": please remove this assignment; " + suffix;
    }

    @Override
    public synchronized Enumeration<?> propertyNames()
    {
        return Collections.enumeration(stringPropertyNames());
    }

    @Override
    public synchronized Set<String> stringPropertyNames()
    {
        Set<String> names = new HashSet<String>();
        for (String name: super.stringPropertyNames()) {
            if( !name.startsWith(STORAGE_PREFIX_ANNOTATIONS) &&
                !name.startsWith(STORAGE_PREFIX_ERROR_MSG)) {
                names.add(name);
            }
        }
        return names;
    }

    public String replaceKeywords(String s)
    {
        return Formats.replaceKeywords(s, _replaceable);
    }

    public String getValue(String name)
    {
        String value = getProperty(name);
        return (value == null) ? null : replaceKeywords(value);
    }

    private void store(AnnotatedKey key) {
        String name = key.getPropertyName();

        setProperty(STORAGE_PREFIX_ANNOTATIONS + name,
                    key.getAnnotationDeclaration());

        setProperty(STORAGE_PREFIX_ERROR_MSG + name,
                    key.getError());
    }



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
    public static class AnnotatedKey {

        private static final String RE_ATTRIBUTE = "[-\\w]+";
        private static final String RE_SEPARATOR = ",";
        private static final String RE_ANNOTATION_DECLARATION =
            "(\\((" + RE_ATTRIBUTE + "(?:" + RE_SEPARATOR + RE_ATTRIBUTE + ")*)\\))";
        private static final String RE_KEY_DECLARATION =
            RE_ANNOTATION_DECLARATION + "([-\\w.]+)";

        private static final Pattern PATTERN_ANNOTATION_DECLARATION = Pattern.compile(RE_ANNOTATION_DECLARATION);
        private static final Pattern PATTERN_KEY_DECLARATION = Pattern.compile(RE_KEY_DECLARATION);
        private static final Pattern PATTERN_SEPARATOR = Pattern.compile(RE_SEPARATOR);

        private static final Set<Annotation> FORBIDDEN_OBSOLETE_DEPRECATED =
            EnumSet.of(Annotation.FORBIDDEN, Annotation.OBSOLETE, Annotation.DEPRECATED);

        private static final EnumSet<Annotation> FORBIDDEN_OBSOLETE =
            EnumSet.of(Annotation.FORBIDDEN, Annotation.OBSOLETE);

        private final String _name;
        private final String _annotationDeclaration;
        private final Set<Annotation> _annotations = EnumSet.noneOf(Annotation.class);
        private final String _error;

        public AnnotatedKey(String name, String annotationDeclaration, String error) {
            _name = name;
            _error = error;
            _annotationDeclaration = annotationDeclaration;

            Matcher m = PATTERN_ANNOTATION_DECLARATION.matcher(annotationDeclaration);
            if( !m.matches()) {
                throw new IllegalStateException("Cannot match stored annotation declaration");
            }
            for(String label : PATTERN_SEPARATOR.split(m.group(2))) {
                _annotations.add(Annotation.forLabel(label));
            }
        }

        public AnnotatedKey(Object propertyKey, Object propertyValue) {
            String declaration = propertyKey.toString();
            Matcher m = PATTERN_KEY_DECLARATION.matcher(declaration);
            if(m.matches()) {
                _annotationDeclaration = m.group(1);

                for(String label : PATTERN_SEPARATOR.split(m.group(2))) {
                    _annotations.add(Annotation.forLabel(label));
                }

                _name = m.group(3);

                if(countDeclaredAnnotationsFrom(FORBIDDEN_OBSOLETE_DEPRECATED) > 1) {
                    throw new IllegalArgumentException("At most one of forbidden, obsolete " +
                            "and deprecated may be specified.");
                }
            } else {
                _annotationDeclaration = "";
                _name = declaration;
            }

            _error = hasAnyOf(FORBIDDEN_OBSOLETE) ? propertyValue.toString() : "";
        }

        private int countDeclaredAnnotationsFrom(Set<Annotation> items) {
            EnumSet<Annotation> a = EnumSet.copyOf(items);
            a.retainAll(_annotations);
            return a.size();
        }

        public boolean hasAnnotation(Annotation annotation) {
            return _annotations.contains(annotation);
        }

        public boolean hasAnyOf(EnumSet<Annotation> annotations) {
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
    }


    /**
     *  This enum represents a property key annotation.  Each annotation has
     *  an associated label that is present as a comma-separated list within
     *  parentheses.
     */
    public enum Annotation {
        FORBIDDEN("forbidden"),
        OBSOLETE("obsolete"),
        DEPRECATED("deprecated"),
        NOT_FOR_SERVICES("not-for-services");

        private static Map<String,Annotation> ANNOTATION_LABELS = new HashMap<String,Annotation>();

        private final String _label;

        static {
            for( Annotation annotation : Annotation.values()) {
                ANNOTATION_LABELS.put(annotation._label, annotation);
            }
        }

        public static Annotation forLabel(String label) {
            checkArgument(ANNOTATION_LABELS.containsKey(label), "Unknown annotation " + label);
            return ANNOTATION_LABELS.get(label);
        }

        Annotation(String label) {
            _label = label;
        }
    }
}
