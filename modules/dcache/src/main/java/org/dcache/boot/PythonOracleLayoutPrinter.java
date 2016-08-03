package org.dcache.boot;

import com.google.common.base.Strings;

import java.io.PrintStream;

import org.dcache.util.ConfigurationProperties;

/**
 * Creates a Python declaration for a class that is an oracle for
 * consulting the value of dCache properties.  By executing the
 * output ('exec' python command) a class 'Properties' is defined
 * along with an instance of this class called 'properties'.
 *
 * The get method of 'properties' returns the value of a property, or
 * the python literal 'None' if it isn't defined.  The get method takes
 * one mandatory argument (the property name) and two optional ones: the
 * name of a domain and the name of a service (as provided by the
 * '<service>.cell.name' property).
 *
 * Here is a simple python script that demonstrates these features.
 *
 *     #!/usr/bin/env python
 *
 *     from subprocess import Popen, PIPE
 *     d = Popen(["dcache", "loader", "-q", "compile", "-python"], stdout=PIPE)
 *     exec d.communicate()[0]
 *
 *     #  Print the value of a globally defined property
 *     print properties.get('info-provider.se-unique-id')
 *
 *     #  Print the value of a property for a specific domain
 *     print properties.get('dcache.java.memory.heap', 'dCacheDomain')
 *
 *     #  Print the value of a property for a specific service-instance
 *     print properties.get('path', 'dCacheDomain', 'pool1')
 *     print properties.get('path', 'dCacheDomain', 'pool2')
 */
public class PythonOracleLayoutPrinter implements LayoutPrinter
{

    private final Layout _layout;

    public PythonOracleLayoutPrinter(Layout layout)
    {
        _layout = layout;
    }

    @Override
    public void print(PrintStream out)
    {
        new Request(out).print();
    }

    private class Request
    {
        private final PrintStream _out;

        public Request(PrintStream out)
        {
            _out = out;
        }

        public void print()
        {
            IndentPrinter base = new IndentPrinter(_out);
            base.println("class Properties:");
            printClass(base.indent());
            base.println("properties = Properties()");
        }

        public void printClass(IndentPrinter out)
        {
            out.println("\"\"\"Allows queries against dCache configuration\"\"\"");
            out.println("def __init__(self):");
            printInit(out.indent());
            out.println("def get(self, key, domain=None, service=None):");
            printGetDomainService(out.indent());
        }

        private void printGetDomainService(IndentPrinter out)
        {
            IndentPrinter indent = out.indent();

            out.println("if domain in self.service_scope and \\");
            out.println("      service in self.service_scope [domain] and \\");
            out.println("      key in self.service_scope [domain] [service]:");
            indent.println("return self.service_scope [domain] [service] [key]");
            out.println();
            out.println("if domain in self.domain_scope and \\");
            out.println("      key in self.domain_scope[domain]:");
            indent.println("return self.domain_scope [domain] [key]");
            out.println();
            out.println("if key in self.global_scope:");
            indent.println("return self.global_scope[key]");
            out.println();
            out.println("return None");
            out.println();
        }

        private void printInit(IndentPrinter out)
        {
            IndentPrinter indent = out.indent();

            out.println("self.global_scope = {");
            printGlobalScope(indent);
            indent.println("}");
            out.println();
            out.println("self.domain_scope = {");
            printDomainScope(indent);
            indent.println("}");
            out.println();
            out.println("self.service_scope = {");
            printServiceScope(indent);
            indent.println("}");
            out.println();
        }

        private void printGlobalScope(IndentPrinter out)
        {
            ConfigurationProperties properties = _layout.properties();

            for(String key : properties.stringPropertyNames()) {
                printEntry(out, key, properties.getValue(key));
            }
        }

        private void printDomainScope(IndentPrinter out)
        {
            IndentPrinter indent = out.indent();

            for(Domain domain : _layout.getDomains()) {
                String name = markup(domain.getName());

                out.println("'" + name + "' : {");
                printProperties(indent, domain.properties(),
                        _layout.properties());
                indent.println("},");
            }
        }

        private void printServiceScope(IndentPrinter out)
        {
            IndentPrinter indent = out.indent();

            for(Domain domain : _layout.getDomains()) {
                String name = markup(domain.getName());
                out.println("'" + name + "' : {");
                printDomainServices(indent, domain);
                indent.println("},");
            }
        }

        private void printDomainServices(IndentPrinter out, Domain domain)
        {
            IndentPrinter indent = out.indent();

            for(ConfigurationProperties service : domain.getServices()) {
                String name = Properties.getCellName(service);
                if (!Strings.isNullOrEmpty(name)) {
                    out.println("'" + markup(name) + "' : {");
                    printProperties(indent, service, domain.properties());
                    indent.println("},");
                }
            }
        }

        private void printProperties(IndentPrinter out,
                ConfigurationProperties properties,
                ConfigurationProperties parentProperties)
        {
            for(String key : properties.stringPropertyNames()) {
                String value = properties.getValue(key);

                if(!value.equals(parentProperties.getValue(key))) {
                    printEntry(out, key, value);
                }
            }
        }

        private void printEntry(IndentPrinter out, String key, String value)
        {
            String safeKey = markup(key);
            String safeValue = markup(value);
            out.println("'" + safeKey + "' : '" + safeValue + "',");
        }
    }


    private static String markup(String in)
    {
        StringBuilder sb = new StringBuilder(in.length());

        for(char c : in.toCharArray()) {
            switch(c) {
                case '\n':
                    sb.append('\\');
                    c = 'n';
                    break;
                case '\t':
                    sb.append('\\');
                    c = 't';
                    break;
                case '\\':
                case '\'':
                    sb.append('\\');
                    break;
            }
            sb.append(c);
        }

        return sb.toString();
    }

    /**
     * Simple class to assist with printing Python code with the correct
     * indentation.
     */
    private static class IndentPrinter
    {
        private final PrintStream _inner;
        private final String _indent;

        public IndentPrinter(PrintStream inner)
        {
            this(inner, "");
        }

        private IndentPrinter(PrintStream inner, String indent)
        {
            _inner = inner;
            _indent = indent;
        }

        public void println()
        {
            _inner.println();
        }

        public void println(String line)
        {
            if(line.isEmpty()) {
                _inner.println();
            } else {
                _inner.println(_indent + line);
            }
        }

        public IndentPrinter indent()
        {
            return new IndentPrinter(_inner, _indent + "   ");
        }
    }
}
