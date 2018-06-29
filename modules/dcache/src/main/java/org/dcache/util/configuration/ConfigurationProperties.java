package org.dcache.util.configuration;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import dmg.util.Formats;
import dmg.util.PropertiesBackedReplaceable;

import static com.google.common.base.Preconditions.checkNotNull;

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
 * Annotations are inherited when several ConfigurationProperties instances are
 * chained. They are however not inherited through non-ConfigurationProperties
 * classes, eg
 * new ConfigurationProperties(new Properties(new ConfigurationProperties()))
 * will not preserve annotations.
 *
 * <p>
 * The following provides examples of valid annotated declarations:
 * <pre>
 *   (obsolete)dcache.property1 = some value
 *   (forbidden)dcache.property2 =
 *   (deprecated,no-for-services)dcache.property3 = default-value
 * </pre>
 *
 * @see Properties
 */
public class ConfigurationProperties
    extends Properties
{
    private static final long serialVersionUID = -5684848160314570455L;

    /**
     * The character that separates the prefix from the key for PREFIX-annotated
     * properties.
     */
    public static final String PREFIX_SEPARATOR = "!";

    private static final Set<Annotation> OBSOLETE_FORBIDDEN =
        EnumSet.of(Annotation.OBSOLETE, Annotation.FORBIDDEN);

    private static final Pattern MATCH_COMMAS = Pattern.compile(",");

    private final PropertiesBackedReplaceable _replaceable =
        new PropertiesBackedReplaceable(this);

    private final Map<String,AnnotatedKey> _annotatedKeys =
            new HashMap<>();
    private final UsageChecker _usageChecker;
    private final List<String> _prefixes = new ArrayList<>();

    private boolean _loading;
    private boolean _isService;
    private ProblemConsumer _problemConsumer = new DefaultProblemConsumer();

    public ConfigurationProperties()
    {
        super();
        _usageChecker = new UniversalUsageChecker();
    }

    public ConfigurationProperties(Properties defaults)
    {
        this(defaults, new UniversalUsageChecker());
    }

    public ConfigurationProperties(Properties defaults, UsageChecker usageChecker)
    {
        super(defaults);

        if( defaults instanceof ConfigurationProperties) {
            ConfigurationProperties defaultConfig = (ConfigurationProperties) defaults;
            _problemConsumer = defaultConfig._problemConsumer;
            _prefixes.addAll(defaultConfig._prefixes);
        }
        _usageChecker = usageChecker;
    }

    public void setProblemConsumer(ProblemConsumer consumer)
    {
        _problemConsumer = consumer;
    }

    public ProblemConsumer getProblemConsumer()
    {
        return _problemConsumer;
    }

    public void setIsService(boolean isService)
    {
        _isService = isService;
    }

    public boolean hasDeclaredPrefix(String name)
    {
        for (String prefix : _prefixes) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }

        return false;
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
        try (Reader reader = new FileReader(file)) {
            load(file.getName(), 0, reader);
        }
    }

    /**
     * Wrapper method that ensures error and warning messages have
     * the correct line number.
     * @param source a label describing where Reader is obtaining information
     * @param line Number of lines read so far
     * @param reader Source of the property information
     */
    public void load(String source, int line, Reader reader) throws IOException
    {
        LineNumberReader lnr = new LineNumberReader(reader);
        lnr.setLineNumber(line);
        load(source, lnr);
    }

    /**
     * Wrapper method that ensures error and warning messages have
     * the correct line number.
     * @param source a label describing where Reader is obtaining information
     * @param reader Source of the property information
     */
    public void load(String source, LineNumberReader reader) throws IOException
    {
        _problemConsumer.setFilename(source);
        _problemConsumer.setLineNumberReader(reader);
        try {
            load(new ConfigurationParserAwareReader(reader));
        } finally {
            _problemConsumer.setFilename(null);
        }
    }

    /**
     * @throws IllegalArgumentException during loading if key is
     * already defined.
     */
    @Override
    public synchronized Object put(Object rawKey, Object value)
    {
        checkNotNull(rawKey, "A property key must not be null");
        checkNotNull(value, "A property value must not be null");

        AnnotatedKey key = new AnnotatedKey(rawKey, value);
        String name = key.getPropertyName();
        if (_loading && containsKey(name)) {
            _problemConsumer.error(name + " is already defined");
            return null;
        }

        if (key.hasAnnotation(Annotation.PREFIX)) {
            _prefixes.add(key.getPropertyName() + PREFIX_SEPARATOR);
        }

        checkIsAllowed(key, (String) value);

        if (key.hasAnnotations()) {
            putAnnotatedKey(key);
        }

        return key.hasAnyOf(OBSOLETE_FORBIDDEN) ? null : super.put(name, canonicalizeValue(name, value));
    }

    private String canonicalizeValue(String key, Object value)
    {
        AnnotatedKey annotatedKey = getAnnotatedKey(key);
        if (annotatedKey != null && annotatedKey.hasAnnotation(Annotation.ANY_OF)) {
            return MATCH_COMMAS.splitAsStream(String.valueOf(value))
                               .map(String::trim)
                               .filter(s -> !s.isEmpty())
                               .distinct()
                               .sorted()
                               .collect(Collectors.joining(","));
        }

        return String.valueOf(value).trim();
    }


    protected void checkIsAllowed(AnnotatedKey key, String value)
    {
        String name = key.getPropertyName();
        AnnotatedKey existingKey = getAnnotatedKey(name);
        if (existingKey != null) {
            checkKeyValid(existingKey, key);
            checkDataValid(existingKey, value);
        } else if (name.indexOf('/') > -1) {
            _problemConsumer.error(
                    "Property " + name + " is a scoped property. Scoped properties are no longer supported.");
        } else if (!_usageChecker.isStandardProperty(defaults, name)) {
            // TODO: It would be nice if we could check whether the property is actually
            // used, ie if it appears as part of the value of a standard property. To do this
            // we need to implement a multi-pass parser and that means rewriting
            // the entire property checking logic.
            _problemConsumer.info("Property " + name + " is not a standard property");
        }
        checkDataValid(key, value);
    }

    private void checkKeyValid(AnnotatedKey existingKey, AnnotatedKey key)
    {
        String name = key.getPropertyName();

        if (existingKey.hasAnnotations() && key.hasAnnotations()) {
            _problemConsumer.error("Property " + name + ": " +
                    "remove \"" + key.getAnnotationDeclaration() + "\"; " +
                    "annotated assignments are not allowed");
        }

        if (existingKey.hasAnyOf(EnumSet.of(Annotation.IMMUTABLE,
                Annotation.PREFIX, Annotation.FORBIDDEN))) {
            _problemConsumer.error(messageFor(existingKey));
        }

        if ((_isService && existingKey.hasAnnotation(Annotation.NOT_FOR_SERVICES)) ||
            existingKey.hasAnyOf(EnumSet.of(Annotation.OBSOLETE, Annotation.DEPRECATED))) {
            _problemConsumer.warning(messageFor(existingKey));
        }
    }


    private void checkDataValid(AnnotatedKey key, String value)
    {
        if(key.hasAnnotation(Annotation.ONE_OF)) {
            String oneOfParameter = key.getParameter(Annotation.ONE_OF);
            Set<String> validValues = ImmutableSet.copyOf(oneOfParameter.split("\\|"));
            if(!validValues.contains(value)) {
                String validValuesList = "\"" +
                        Joiner.on("\", \"").join(validValues) + "\"";
                _problemConsumer.error("Property " + key.getPropertyName() +
                        ": \"" + value + "\" is not a valid value.  Must be one of "
                        + validValuesList);
            }
        }
        if (key.hasAnnotation(Annotation.ANY_OF)) {
            String anyOfParameter = key.getParameter(Annotation.ANY_OF);
            Set<String> values = Sets.newHashSet(Splitter.on(',')
                    .omitEmptyStrings()
                    .trimResults().split(value));
            Set<String> validValues = ImmutableSet.copyOf(anyOfParameter.split("\\|"));
            values.removeAll(validValues);
            if (!values.isEmpty()) {
                String validValuesList = "\""
                        + Joiner.on("\", \"").join(validValues) + "\"";
                _problemConsumer.error("Property " + key.getPropertyName()
                        + ": \"" + value
                        + "\" is not a valid value. Must be a comma separated list of "
                        + validValuesList);
            }
        }
    }

    /**
     * Define the binary relationship property A hasSynonym property B
     * as true iff either:
     * <ul>
     * <li>If there exists precisely one non-deprecated property with a simple reference to
     * property A; e.g.
     * <pre>
     *     property.B = ${property.A}
     *     property.A = some default value
     * </pre>
     * <li>If there exists precisely one deprecated property that hasSynonym property B
     * with a simple reference to property A; e.g.
     * <pre>
     *     property.B = ${property.C}
     *     (deprecated)property.C = ${property.A}
     *     property.A = some default value
     * </pre>
     * <p>
     * This method returns the name of a property that is the subject of
     * hasSynonym relationship (property B) with the supplied property
     * (as property A), or null if no such property exists.
     */
    private String findSynonymOf(String propertyName)
    {
        String synonym = null;
        String simpleReference = "${" + propertyName + "}";

        for (String name : stringPropertyNames()) {
            String value = getProperty(name);
            if (value.equals(simpleReference)) {
                if (synonym != null) {
                    return null;
                }
                synonym = name;
            }
        }

        AnnotatedKey key = getAnnotatedKey(synonym);
        if (key != null && key.hasAnnotation(Annotation.DEPRECATED)) {
            synonym = findSynonymOf(synonym);
        }

        return synonym;
    }

    private String messageFor(AnnotatedKey key)
    {
        String name = key.getPropertyName();

        StringBuilder sb = new StringBuilder();
        sb.append("Property ").append(name).append(": ");

        if (key.hasAnnotation(Annotation.IMMUTABLE)) {
            sb.append("may not be adjusted as it is marked 'immutable'");
        } else if (key.hasAnnotation(Annotation.PREFIX)) {
            sb.append("may not be adjusted as it is marked 'prefix'");
        } else if (key.hasAnnotation(Annotation.FORBIDDEN)) {
            sb.append("may not be adjusted; ");
            sb.append(key.hasError() ? key.getError() : "this property no longer affects dCache");
        } else if (key.hasAnnotation(Annotation.OBSOLETE)) {
            sb.append("please remove this assignment; ");
            sb.append(key.hasError() ? key.getError() : "it has no effect");
        } else if(key.hasAnnotation(Annotation.DEPRECATED)) {
            String synonym = findSynonymOf(name);
            if (synonym != null) {
                sb.append("use \"").append(synonym).append("\" instead");
            } else {
                sb.append("please review configuration");
            }
            sb.append("; support for ").append(name).append(" will be removed in the future");
        } else if (key.hasAnnotation(Annotation.NOT_FOR_SERVICES)) {
            sb.append("consider moving to a domain scope; it has no effect here");
        } else {
            sb.append("has an unknown problem");
        }

        return sb.toString();
    }

    @Override
    public synchronized Enumeration<?> propertyNames()
    {
        return Collections.enumeration(stringPropertyNames());
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

    @Nullable
    public AnnotatedKey getAnnotatedKey(String name)
    {
        AnnotatedKey key = _annotatedKeys.get(name);
        if (key == null && defaults instanceof ConfigurationProperties) {
            key = ((ConfigurationProperties) defaults).getAnnotatedKey(name);
        }
        return key;
    }

    private void putAnnotatedKey(AnnotatedKey key)
    {
        _annotatedKeys.put(key.getPropertyName(), key);
    }
}
