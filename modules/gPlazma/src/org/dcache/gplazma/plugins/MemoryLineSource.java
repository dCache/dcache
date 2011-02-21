package org.dcache.gplazma.plugins;

import java.io.IOException;
import java.util.List;

/**
 * encapsulates access to memory based text line sources.
 * @author karsten
 */
class MemoryLineSource implements LineSource {

    private List<String> _content;
    private boolean isModified;

    /**
     * Creates a new source and initialises it with content
     * @param content Content to use for initialisation
     */
    public MemoryLineSource(List<String> content) {
        init(content);
    }

    /**
     * Sets the content of the source
     * @param content
     */
    public void setContent(List<String> content) {
        init(content);
    }

    @Override
    public boolean hasChanged() {
        return isModified;
    }

    @Override
    public List<String> getContent() throws IOException {
        return _content;
    }

    private void init(List<String> content) {
        _content = content;
        isModified = true;
    }
}
