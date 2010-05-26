package org.dcache.boot;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dcache.util.DeprecatableProperties;
import org.dcache.util.NetworkUtils;
import org.dcache.util.ReplaceableProperties;

/**
 * Layout encapsulates the configuration of a set of domains.
 */
public class Layout
{
    private static final int READ_AHEAD_LIMIT = 256;

    private static final Pattern SECTION_HEADER =
        Pattern.compile("^\\s*\\[([^\\]/]+)(/([^\\]/]+))?\\]\\s*$");

    private final ReplaceableProperties _properties;
    private final Map<String,Domain> _domains =
        new LinkedHashMap<String,Domain>();

    public Layout(ReplaceableProperties config)
    {
        _properties = new DeprecatableProperties(config);
    }

    public ReplaceableProperties properties()
    {
        return _properties;
    }

    public Domain getDomain(String name)
    {
        return _domains.get(name);
    }

    public Domain createDomain(String name)
    {
        Domain domain = _domains.get(name);
        if (domain == null) {
            domain = new Domain(name, _properties);
            _domains.put(name, domain);
        }
        return domain;
    }

    /**
     * Reads a layout definition from the URI.
     *
     * @param uri The URI of the layout definition.
     */
    public void load(URI uri)
        throws URISyntaxException, IOException
    {
        URL url = NetworkUtils.toURL(uri);
        LineNumberReader reader =
            new LineNumberReader(new InputStreamReader(url.openStream()));
        try {
            load(reader);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("%s, line %d: %s", uri, reader.getLineNumber(), e.getMessage()), e);
        } catch (IOException e) {
            throw new IOException(String.format("%s, line %d: %s", uri, reader.getLineNumber(), e.getMessage()), e);
        } catch (RuntimeException e) {
            throw new RuntimeException(String.format("%s, line %d: %s", uri, reader.getLineNumber(), e.getMessage()), e);
        } finally {
            reader.close();
        }
    }

    /**
     * Reads a layout definition from the input character stream.
     *
     * @param reader the input character stream.
     */
    public void load(BufferedReader reader)
        throws IOException
    {
        loadSection(reader, _properties);

        String s;
        while ((s = reader.readLine()) != null) {
            Matcher matcher = SECTION_HEADER.matcher(s);
            if (!matcher.matches()) {
                throw new RuntimeException("Bug detected: Section header expected: " + s);
            }

            String domainName =
                _properties.replaceKeywords(matcher.group(1));
            String serviceName =
                matcher.group(3);

            if (serviceName == null) {
                Domain domain = createDomain(domainName);
                loadSection(reader, domain.properties());
            } else {
                Domain domain = getDomain(domainName);
                if (domain == null) {
                    throw new IllegalArgumentException(String.format("Service declaration %s/%s lacks definition of domain %s", domainName, serviceName, domainName));
                }
                loadSection(reader, domain.createService(serviceName));
            }
        }
    }

    /**
     * Reads properties until the next section header. The position is
     * advanced until the next section header or the end of file.
     *
     * @param reader The reader to read from
     * @param config The Properties to which to add the properties
     */
    private void loadSection(BufferedReader reader, Properties config)
        throws IOException
    {
        String s;
        StringBuilder section = new StringBuilder();
        reader.mark(READ_AHEAD_LIMIT);
        while ( (s = reader.readLine()) != null &&
                !SECTION_HEADER.matcher(s).matches()) {
            section.append(s).append('\n');
            reader.mark(READ_AHEAD_LIMIT);
        }
        reader.reset();
        config.load(new StringReader(section.toString()));
    }

    /**
     * Prints the list of domains in the layout.
     */
    public void printDomainNames(PrintStream out)
    {
        for (Domain domain: _domains.values()) {
            out.println(domain.getName());
        }
    }

    /**
     * Prints the domains in the layout matching one of the
     * patterns.
     */
    public void printMatchingDomainNames(PrintStream out, Collection<Pattern> patterns)
    {
        for (Domain domain: _domains.values()) {
            for (Pattern pattern: patterns) {
                String name = domain.getName();
                if (pattern.matcher(name).matches()) {
                    out.println(name);
                    break;
                }
            }
        }
    }
}
