package org.dcache.boot;

import com.google.common.base.Strings;

import java.io.PrintStream;
import java.util.Enumeration;
import java.util.Map;

import org.dcache.util.ConfigurationProperties;

/**
 * Provide a clean view of the configuration properties.  This is intended to
 * be used for diagnosing problems with configuration.
 * <p/>
 * Each property assignment is listed as either a one- or two-line form.  In
 * both cases, the first line is like:
 *
 * <pre>
 *     &lt;key> = &lt;value>
 * </pre>
 *
 * If the value is derived from other property values then the definition
 * without any expansion is printed on the following line as a comment:
 *
 * <pre>
 *     &lt;key> = &lt;value>
 *     #        &lt;unexpanded-value>
 * </pre>
 *
 * The output format is (deliberately) similar to a dCache configuration file,
 * with domain and service declarations and context-specific assignments.
 * However, please note that, unlike dCache configuration files, there is no
 * encoding of special characters.  This means that the output from this class
 * cannot be used as valid dCache configuration.
 */
public class DebugLayoutPrinter implements LayoutPrinter
{

    private final Layout _layout;
    private PrintStream _out;

    public DebugLayoutPrinter(Layout layout)
    {
        _layout = layout;
    }

    @Override
    public void print(PrintStream out)
    {
        _out = out;
        out.println("#        --- DEFAULTS ---\n");

        listDefaults(_layout.properties());

        out.println("\n#        --- CONFIGURATION ---\n");

        listProperties(_layout.properties(), "", 0);

        for (Domain domain : _layout.getDomains()) {
            listProperties(domain.properties(),
                    "[" + domain.getName() + "]", 2);

            for (ConfigurationProperties properties :
                    domain.getServices()) {
                listProperties(properties, "[" + domain.getName() + "/"
                        + properties.getValue(Properties.PROPERTY_DOMAIN_SERVICE) + "]", 2);
            }
        }
    }

    private void listDefaults(ConfigurationProperties properties)
    {
        Enumeration<?> propertyNames = properties.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String key = (String) propertyNames.nextElement();
            if (!properties.contains(key)) {
                showProperty(properties, 0, key);
            }
        }
    }

    private void listProperties(ConfigurationProperties properties,
            String label, int indent)
    {
        _out.println();

        if (!label.isEmpty()) {
            _out.println(Strings.padStart(label, indent, ' '));
        }

        int assignmentIndent = indent > 0 ? indent + 1 : 0;

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            showProperty(properties, assignmentIndent, key);
        }
    }

    private void showProperty(ConfigurationProperties properties, int indent,
            String key)
    {
        String rawValue = properties.getProperty(key);
        _out.format("%s = %s\n", Strings.padStart(key, indent+key.length(),' '),
                rawValue);

        String value = properties.getValue(key);
        if (!rawValue.equals(value)) {
            _out.format("#%s\n", Strings.padStart(value,
                    indent+key.length()+2+value.length(), ' '));
        }
    }
}
