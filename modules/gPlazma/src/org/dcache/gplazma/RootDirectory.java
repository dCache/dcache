package org.dcache.gplazma;

/**
 *
 * @author timur
 */
public class RootDirectory implements SessionAttribute {

    public String directory;
    public RootDirectory(String directory) {
        this.directory = directory;
    }
    public String getName() {
        return directory;
    }

    public Object getValue() {
        return directory;
    }

}
