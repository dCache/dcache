package org.dcache.gplazma;
import java.io.Serializable;

/**
 *
 * @author timur
 */
public class RootDirectory implements SessionAttribute,Serializable {
    private static final long serialVersionUID = -8777503035832810959L;

    public String directory;

    public RootDirectory(String directory) {
       if(directory == null) {
            throw new NullPointerException("directory is null");
        }
        this.directory = directory;
    }

    @Override
    public String getName() {
        return "RootDirectory";
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
        final RootDirectory other = (RootDirectory) obj;
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
