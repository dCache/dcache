package org.dcache.gplazma.plugins;

import java.io.IOException;
import java.util.List;

/**
 * Interface for text lines providing sources.
 * @author karsten
 */
interface LineSource {

    /**
     * @return True if a subsequent call to getContent will return new content, false otherwise.
     */
    boolean hasChanged();

    /**
     * @return Complete content of the source
     * @throws IOException
     */
    List<String> getContent() throws IOException;

}
