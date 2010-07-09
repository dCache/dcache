package org.dcache.gplazma;
import java.io.Serializable;

/**
 *
 * @author timur
 */
public class ReadOnly implements SessionAttribute, Serializable {
    private static final long serialVersionUID = -1938337009096168711L;

    public boolean readOnly;

    public ReadOnly(String readOnly) {
        this(Boolean.valueOf(readOnly));
    }

    public ReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public String getName() {
        return "ReadOnly";
    }

    @Override
    public Object getValue() {
        return Boolean.valueOf(readOnly);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ReadOnly other = (ReadOnly) obj;
        return this.readOnly == other.readOnly;
    }

    @Override
    public int hashCode() {
        return Boolean.valueOf(readOnly).hashCode();
    }

    @Override
    public String toString() {
        return readOnly ? "ReadOnly" : "ReadWrite";
    }

}
