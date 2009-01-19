package org.dcache.services.hsmcleaner;

import java.io.FilenameFilter;
import java.io.File;
import java.util.regex.Pattern;

/**
 * Implementation of FilenameFilter interface using a regular
 * expression for matching file names.
 */
public class PatternFilenameFilter implements FilenameFilter
{
    final Pattern _pattern;

    public PatternFilenameFilter(String regex)
    {
        _pattern = Pattern.compile(regex);
    }

    public boolean accept(File dir, String name) 
    {
        return _pattern.matcher(name).matches();
    }
}
