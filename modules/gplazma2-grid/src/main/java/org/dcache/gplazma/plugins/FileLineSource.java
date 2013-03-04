package org.dcache.gplazma.plugins;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Encapsulates access to a line based text file.
 * @author karsten
 *
 */
class FileLineSource implements LineSource {

    private final File _file;
    private final long _minCheckInterval;
    private long _lastRefresh;

    /**
     * @param filepath Path to text file
     * @param minCheckIntervalMillis minimum interval to check the file's modification time
     * @throws FileNotFoundException
     */
    public FileLineSource(String filepath, long minCheckIntervalMillis) throws FileNotFoundException {
        _file = new File(filepath);
        _minCheckInterval = minCheckIntervalMillis;
        if (!_file.exists()) {
            throw new FileNotFoundException(String
                    .format("File '%s' not found.", filepath));
        }
    }

    @Override
    public boolean hasChanged() {
        return (_lastRefresh+_minCheckInterval < System.currentTimeMillis()) && (_lastRefresh < _file.lastModified());
    }

    @Override
    public List<String> getContent() throws IOException {
        List<String> result = Files.readLines(_file, Charsets.UTF_8);
        _lastRefresh = System.currentTimeMillis();
        return result;
    }
}
