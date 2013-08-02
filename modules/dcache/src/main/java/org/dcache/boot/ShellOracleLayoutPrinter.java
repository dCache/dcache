package org.dcache.boot;

import java.io.PrintStream;

import org.dcache.util.ConfigurationProperties;

/**
 * Generates a shell function {@code getProperty} which serves as
 * an oracle for property values. The function takes three
 * arguments: property key, domain name, cell name. The latter two
 * are optional.
 *
 * For undefined cells the function calls {@code
 * undefinedCell}. For undefined domains the function calls {@code
 * undefinedDomain}. For undefined properties the function calls
 * {@code undefinedProperties}. If those functions return then
 * processing falls through to the enclosing configuration
 * context.
 */
public class ShellOracleLayoutPrinter implements LayoutPrinter {

    private final Layout _layout;

    public ShellOracleLayoutPrinter(Layout layout) {
        _layout = layout;
    }

    @Override
    public void print(PrintStream out) {
        out.println("getProperty()");
        out.println("{");

        // Logic for per service parameters
        out.println("  case \"$2\" in");
        out.println("    \"\")");
        out.println("      ;;"); // Fall through
        for (Domain domain: _layout.getDomains()) {
            out.append("    ").append(quoteForCase(domain.getName())).println(")");
            out.println("      case \"$3\" in");
            out.println("        \"\")");
            out.println("          ;;"); // Fall through
            for (ConfigurationProperties service: domain.getServices()) {
                String cellName = Properties.getCellName(service);
                if (cellName != null) {
                    out.append("        ").append(quoteForCase(cellName)).println(")");
                    compile(out, "          ", service, domain.properties());
                    out.println("          ;;");
                }
            }
            out.println("        *)");
            out.println("          undefinedCell \"$@\"");
            out.println("          ;;");
            out.println("      esac");
            out.println("      ;;");
        }
        out.println("    *)");
        out.println("      undefinedDomain \"$@\"");
        out.println("      ;;");
        out.println("  esac");
        out.println();

        // Logic for per domain parameters
        out.println("  case \"$2\" in");
        for (Domain domain: _layout.getDomains()) {
            out.append("    ").append(quoteForCase(domain.getName())).println(")");
            compile(out, "      ", domain.properties(), _layout.properties());
            out.println("      ;;");
        }
        out.println("  esac");
        out.println();

        // Logic for global properties
        compile(out, "  ", _layout.properties(), new ConfigurationProperties());
        out.println();

        // Global fallback
        out.println("  undefinedProperty \"$@\"");
        out.println("}");
    }

    private static String quote(String s)
    {
        char[] output = new char[2 * s.length()];
        int len = 0;
        for (char c: s.toCharArray()) {
            switch (c) {
            case '\\':
            case '$':
            case '`':
            case '"':
                output[len++] = '\\';
                break;
            }
            output[len++] = c;
        }
        return new String(output, 0, len);
    }

    private static String quoteForCase(String s)
    {
        char[] output = new char[2 * s.length()];
        int len = 0;
        for (char c: s.toCharArray()) {
            switch (c) {
            case '\\':
            case '$':
            case '`':
            case '"':
            case ')':
            case '?':
            case '*':
            case '[':
                output[len++] = '\\';
                break;
            }
            output[len++] = c;
        }
        return new String(output, 0, len);
    }

    /**
     * Generates a shell case statement for value in {@code
     * properties} not defined in {@code parentProperties}.
     */
    private static void compile(PrintStream out, String indent,
                                ConfigurationProperties properties,
                                ConfigurationProperties parentProperties)
    {
        out.append(indent).println("case \"$1\" in");
        for (String key: properties.stringPropertyNames()) {
            ConfigurationProperties.AnnotatedKey annotatedKey = properties.getAnnotatedKey(key);
            if (annotatedKey == null ||
                    !annotatedKey.hasAnnotation(ConfigurationProperties.Annotation.DEPRECATED)) {
                String value = properties.getValue(key);
                if (!value.equals(parentProperties.getValue(key))) {
                    out.append(indent).append("  ");
                    out.append(quoteForCase(key)).append(") echo \"");
                    out.append(quote(value.trim()));
                    out.println("\"; return;;");
                }
            }
        }
        out.append(indent).println("esac");
    }
}
