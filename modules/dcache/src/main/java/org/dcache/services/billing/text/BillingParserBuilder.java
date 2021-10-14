package org.dcache.services.billing.text;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Multimaps.filterValues;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builder for creating parsers for billing file entries.
 */
public class BillingParserBuilder {

    private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile("\\$(.+?)\\$");

    private ImmutableSetMultimap<String, Pattern> patternsByAttribute;
    private ImmutableSetMultimap<Pattern, String> attributesByPattern;
    private Map<String, String> formats;

    private final Set<String> attributes = new LinkedHashSet<>();
    private boolean canOutputArray = true;

    public BillingParserBuilder(Map<String, String> formats) {
        this.formats = Maps.newHashMap(formats);
        attributesByPattern = toPatterns(formats);
        patternsByAttribute = attributesByPattern.inverse();
    }

    public BillingParserBuilder addAttribute(String attribute) {
        attributes.add(attribute);
        return this;
    }

    public BillingParserBuilder addAllAttributes() {
        attributes.clear();
        attributes.addAll(patternsByAttribute.keySet());
        canOutputArray = false;
        return this;
    }

    public BillingParserBuilder withFormat(String message, String format) {
        formats.put(message, format);
        attributesByPattern = toPatterns(formats);
        patternsByAttribute = attributesByPattern.inverse();
        return this;
    }

    public BillingParserBuilder withFormat(String header) {
        String[] s = header.substring(2).trim().split(" ", 2);
        return (s.length == 2) ? withFormat(s[0], s[1]) : this;
    }

    public Function<String, String> buildToString() {
        String attribute = Iterables.getOnlyElement(attributes);
        String groupName = toGroupName(attribute);
        ImmutableSet<Pattern> patterns = patternsByAttribute.get(attribute);
        return line -> findSingleMatch(line, patterns, groupName);
    }

    public Function<String, Map<String, String>> buildToMap() {
        ImmutableMultimap<Pattern, String> patterns =
              ImmutableMultimap.copyOf(filterValues(attributesByPattern, in(attributes)));
        return line -> findMatchAsMap(line, patterns);
    }

    public Function<String, String[]> buildToArray() {
        checkState(canOutputArray);

        final ImmutableMultimap<Pattern, String> patterns =
              ImmutableMultimap.copyOf(filterValues(attributesByPattern, in(this.attributes)));
        final String[] attributes = this.attributes.toArray(String[]::new);
        return line -> findMatchAsArray(line, patterns, attributes);
    }

    private static String findSingleMatch(String line, ImmutableSet<Pattern> patterns,
          String groupName) {
        Matcher matcher = findMatch(line, patterns);
        return matcher != null ? matcher.group(groupName) : null;
    }

    private static Map<String, String> findMatchAsMap(String line,
          ImmutableMultimap<Pattern, String> patterns) {
        Matcher matcher = findMatch(line, patterns.keySet());
        if (matcher == null) {
            return Collections.emptyMap();
        }
        Map<String, String> values = new HashMap<>();
        for (String attribute : patterns.get(matcher.pattern())) {
            values.put(attribute, matcher.group(toGroupName(attribute)));
        }
        return values;
    }

    private static String[] findMatchAsArray(String line,
          ImmutableMultimap<Pattern, String> patterns,
          String[] attributes) {
        Matcher matcher = findMatch(line, patterns.keySet());
        String[] result = new String[attributes.length];
        if (matcher != null) {
            ImmutableCollection<String> attributesInPattern = patterns.get(matcher.pattern());
            for (int i = 0; i < attributes.length; i++) {
                String attribute = attributes[i];
                if (attributesInPattern.contains(attribute)) {
                    result[i] = matcher.group(toGroupName(attribute));
                }
            }
        }
        return result;
    }

    private static Matcher findMatch(String line, Collection<Pattern> patterns) {
        Matcher result = null;
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                if (result != null) {
                    throw new IllegalArgumentException("Duplicate matches for: " + line);
                }
                result = matcher;
            }
        }
        return result;
    }

    /**
     * Returns Patterns for the provided billing formats, as a Multimap mapping the Pattern to the
     * attributes contained in the pattern.
     */
    private static ImmutableSetMultimap<Pattern, String> toPatterns(Map<String, String> formats) {
        ImmutableSetMultimap.Builder<Pattern, String> builder = ImmutableSetMultimap.builder();
        for (Map.Entry<String, String> format : formats.entrySet()) {
            builder.putAll(toPattern(format.getKey(), format.getValue()),
                  toAttributes(format.getValue()));
        }
        return builder.build();
    }

    /**
     * Returns a Pattern for matching the provided billing format.
     * <p>
     * Attributes are turned into named capturing groups.
     */
    private static Pattern toPattern(String name, String format) {
        StringBuilder regex = new StringBuilder();
        Matcher matcher = ATTRIBUTE_PATTERN.matcher(format);
        int pos = 0;
        while (matcher.find()) {
            if (pos < matcher.start()) {
                regex.append(Pattern.quote(format.substring(pos, matcher.start())));
            }
            String expression = matcher.group(1);
            if (isIf(expression)) {
                regex.append("(?:");
            } else if (isElse(expression)) {
                regex.append("|");
            } else if (isEndIf(expression)) {
                regex.append(")");
            } else {
                regex.append("(?<").append(toGroupName(expression)).append(">");
                // This incomplete list of attribute patterns reduces the risk of false matches
                switch (expression) {
                    case "date":
                        regex.append(".+?");
                        break;
                    case "pnfsid":
                        regex.append("[0-9A-F]{24}(?:[0-9A-F]{12})?");
                        break;
                    case "filesize":
                    case "transferred":
                    case "connectionTime":
                    case "transactionTime":
                    case "queuingTime":
                    case "transferTime":
                    case "rc":
                    case "uid":
                    case "gid":
                        regex.append("-?\\d+");
                        break;
                    case "cached":
                    case "created":
                        regex.append("(?:true|false)");
                        break;
                    case "cellType":
                        switch (name) {
                            case "mover-info-message":
                            case "remove-file-info-message":
                            case "storage-info-message":
                            case "pool-hit-info-message":
                                regex.append("pool");
                                break;
                            case "door-request-info-message":
                                regex.append("door");
                                break;
                            default:
                                regex.append("\\w+");
                                break;
                        }
                        break;
                    case "cellName":
                        regex.append(".+?");
                        break;
                    case "type":
                        switch (name) {
                            case "mover-info-message":
                                regex.append("transfer");
                                break;
                            case "remove-file-info-message":
                                regex.append("remove");
                                break;
                            case "storage-info-message":
                                regex.append("(?:re)?store");
                                break;
                            case "pool-hit-info-message":
                                regex.append("hit");
                                break;
                            case "warning-pnfs-file-info-message":
                                regex.append("warning");
                                break;
                            default:
                                regex.append("\\w+");
                                break;
                        }
                        break;
                    default:
                        regex.append(".*?");
                }
                regex.append(")");
            }
            pos = matcher.end();
        }
        if (pos < format.length()) {
            regex.append(Pattern.quote(format.substring(pos)));
        }
        return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE);
    }

    /**
     * Translates a attribute name into a name suitable for a named capturing group.
     */
    private static String toGroupName(String attribute) {
        int pos = attribute.indexOf(';');
        if (pos > -1) {
            attribute = attribute.substring(0, pos);
        }
        return attribute.replace("X", "XX").replace(".", "X");
    }

    /**
     * Returns names of all attributes in the provided billing format.
     */
    private static Set<String> toAttributes(String format) {
        Set<String> attributes = new HashSet<>();
        Matcher matcher = ATTRIBUTE_PATTERN.matcher(format);
        while (matcher.find()) {
            String expression = matcher.group(1);
            if (!isIf(expression) && !isElse(expression) && !isEndIf(expression)) {
                int pos = expression.indexOf(';');
                attributes.add(pos > -1 ? expression.substring(0, pos) : expression);
            }
        }
        return attributes;
    }

    /**
     * True if the given string template expression is the beginning of an if-expression.
     */
    private static boolean isIf(String expression) {
        return expression.startsWith("if(") && expression.endsWith(")");
    }

    /**
     * True if the given string template expression is the else keyword of an if-expression.
     */
    private static boolean isElse(String expression) {
        return expression.equals("else");
    }

    /**
     * True if the given string template expression is the end of an if-expression.
     */
    private static boolean isEndIf(String expression) {
        return expression.equals("endif");
    }
}
