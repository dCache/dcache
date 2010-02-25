package org.dcache.util;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import java.util.Properties;
import java.util.Stack;
import java.util.Set;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Arrays;
import java.util.Collection;
import java.util.InvalidPropertiesFormatException;
import java.util.regex.Pattern;

import dmg.util.Replaceable;
import dmg.util.Formats;

/**
 * ReplaceableProperties extends the regular Properties class with
 * support for ${...} placeholders.
 *
 * Besides implementing Replaceable, the class extends the load
 * methods in such a way that repeated definitions of the same
 * properties is reported as an error.
 */
public class ReplaceableProperties
    extends Properties
    implements Replaceable
{
    private boolean _loading = false;
    private Stack<String> _replacementStack = new Stack<String>();

    public ReplaceableProperties(Properties properties)
    {
        super(properties);
    }

    /**
     * @throws IllegalArgumentException during loading if a property
     * is defined multiple times.
     */
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
     * is defined multiple times.
     */
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
    public Object put(Object key, Object value)
    {
        if (_loading && containsKey(key)) {
            throw new IllegalArgumentException(String.format("%s is already defined", key));
        }
        return super.put(key, value);
    }

    /**
     * Substitutes all placeholders in a string.
     */
    public String replaceKeywords(String s)
    {
        return Formats.replaceKeywords(s, this);
    }

    /**
     * Returns the value of a property with all placeholders in the
     * value substituted recursively.
     */
    @Override
    public synchronized String getReplacement(String name)
        throws NoSuchElementException
    {
        String value = getProperty(name);
        if (value != null) {
            if (_replacementStack.search(name) == -1) {
                _replacementStack.push(name);
                try {
                    value = replaceKeywords(value);
                } finally {
                    _replacementStack.pop();
                }
            }
        }
        return value;
    }

    public synchronized Set<String>
        matchingStringPropertyNames(Collection<Pattern> patterns)
    {
        Set<String> matchingNames = new HashSet<String>();
        for (String key: stringPropertyNames()) {
            for (Pattern pattern: patterns) {
                if (pattern.matcher(key).matches()) {
                    matchingNames.add(key);
                    break;
                }
            }
        }
        return matchingNames;
    }
}
