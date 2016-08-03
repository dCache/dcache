package org.dcache.boot;

import java.io.PrintStream;
import java.util.Enumeration;

import org.dcache.util.ConfigurationProperties;

/**
 * Print the configuration as a series of statements that define XML
 * entities; for each property there is a line like:
 * <p>
 * <tt>&lt;!ENTITY entityName "entityValue"></tt>
 * <p>
 * The set of valid entity names is a proper subset of the
 * valid property names.  Because of this, some property names are
 * mapped to the nearest valid entity name by substituting invalid
 * characters to the underscore character.  In practice, this
 * shouldn't matter as current naming convention with property names
 * makes this a non-issue.
 * <p>
 * The entityValue is based on the propertyValue and includes all
 * necessary XML markup.
 * <p>
 * The output from this printer may be saved in a file and included
 * in any XML file using the following DTD at the beginning of the
 * file:
 * <pre>
 * &lt;?xml version="1.0" encoding="UTF-8" ?>
 * &lt;!DOCTYPE top-element [
 *   &lt;!ENTITY % properties-data "/path/to/file.xml">
 *   %properties-data;
 * ]>
 * </pre>
 * where <tt>top-element</tt> is the name of the top-most element in
 * the XML file and <tt>/path/to/file.xml</tt> is the path to the
 * file containing this generated data.  With this, parameter values
 * may be referenced using <tt>&amp;entityName;</tt> inside the
 * XML document.
 */
public class XmlEntityLayoutPrinter implements LayoutPrinter {

    private final Layout _layout;

    private final String[] _prefixes;

    /**
     * Create a new XmlEntityLayoutPrinter taking properties from
     * the global context of the supplied layout file.
     * <p>
     * Zero or more prefixes may be provided.  If any prefixes are supplied then
     * a property's key must start with at least one prefix for it to be included
     * in the output.  As a special case, omitting any prefixes will result in
     * all properties being included in the output.
     */
    public XmlEntityLayoutPrinter(Layout layout, String... prefixes) {
        _layout = layout;
        _prefixes = prefixes;
    }

    @Override
    public void print(PrintStream out) {
        ConfigurationProperties properties = _layout.properties();

        Enumeration<?> propertyNames = properties.propertyNames();
        while( propertyNames.hasMoreElements()) {
            String key = (String) propertyNames.nextElement();

            if(isToBePrinted(key)) {
                String value = properties.getValue(key);
                out.println("<!ENTITY " + entityNameFrom(key) + " \"" + entityValueFrom(value) + "\">");
            }
        }
    }

    private boolean isToBePrinted(String key)
    {
        boolean printEntity = _prefixes.length == 0;

        for(String prefix : _prefixes) {
            if(key.startsWith(prefix)) {
                printEntity = true;
                break;
            }
        }

        return printEntity;
    }

    private static String entityNameFrom(String key)
    {
        StringBuilder sb = new StringBuilder();

        char first = key.charAt(0);
        sb.append( isValidEntityNameStartCharacter(first) ? first : "_");

        for( int i = 1; i < key.length(); i++) {
            char c = key.charAt(i);
            sb.append( isValidEntityNameCharacter(c) ? c : "_");
        }

        return sb.toString();
    }


    private static boolean isValidEntityNameStartCharacter( char c)
    {
        if( hasCharType(c, Character.LOWERCASE_LETTER,
                Character.UPPERCASE_LETTER)) {
            return true;
        }
        return c == '_';
    }

    private static boolean isValidEntityNameCharacter( char c)
    {
        if( hasCharType(c, Character.LOWERCASE_LETTER,
                Character.UPPERCASE_LETTER,
                Character.DECIMAL_DIGIT_NUMBER)) {
            return true;
        }
        return c == '-' || c == '.';
    }

    private static boolean hasCharType(char c, int... types)
    {
        int charType = Character.getType(c);
        for (int type : types) {
            if (charType == type) {
                return true;
            }
        }

        return false;
    }

    private static String entityValueFrom( String s)
    {
        return s.replace("&","&amp;").replace("\"", "&quot;").replace("'", "&apos;").replace("<", "&lt;").replace("%", "&#37;");
    }
}
