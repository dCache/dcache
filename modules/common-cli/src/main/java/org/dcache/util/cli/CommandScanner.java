package org.dcache.util.cli;

import java.util.List;
import java.util.Map;

/**
 * Implementations of this interface provide means to analyze
 * an object for supported cell shell commands and create
 * suitable implementations of the CommandExecutor interface.
 */
public interface CommandScanner
{
    /**
     * Scans obj for command definitions.
     *
     * @param obj Object to scan
     * @return Map from command names to CommandExecutors. Each
     * command consists of one or more words.
     */
    Map<List<String>,? extends CommandExecutor> scan(Object obj);
}
