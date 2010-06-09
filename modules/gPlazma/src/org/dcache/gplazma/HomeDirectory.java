package org.dcache.gplazma;

/**
 *
 * @author timur
 */
public class HomeDirectory implements SessionAttribute {

    public String directory;
    public HomeDirectory(String directory) {
        this.directory = directory;
    }
    public String getName() {
        return directory;
    }

    public Object getValue() {
        return directory;
    }

}
