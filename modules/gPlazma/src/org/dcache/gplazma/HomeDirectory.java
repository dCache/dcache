package org.dcache.gplazma;
import java.io.Serializable;

/**
 *
 * @author timur
 */
public class HomeDirectory implements SessionAttribute, Serializable {
    private static final long serialVersionUID = 1088412074483893245L;

    public String directory;

    public HomeDirectory(String directory) {
        if(directory == null) {
            throw new NullPointerException("directory is null");
        }
        this.directory = directory;
    }

    @Override
    public String getName() {
        return "HomeDirectory";
    }

    @Override
    public Object getValue() {
        return directory;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final HomeDirectory other = (HomeDirectory) obj;
        return this.directory.equals(other.directory);
    }

    @Override
    public int hashCode() {
        return this.directory.hashCode();
    }

    @Override
    public String toString() {
        return directory;
    }

}
