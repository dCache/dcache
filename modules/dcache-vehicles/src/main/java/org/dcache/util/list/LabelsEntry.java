package org.dcache.util.list;

import java.io.Serializable;

/**
 *  A LabelsEntry consists of a label name,
 * <p>
 * The LabelsStream interface provides a stream of LabelsEntries.
 */
public class LabelsEntry implements Serializable {

    private static final long serialVersionUID = 9015474311202968086L;

    public final String _name;

    public LabelsEntry(String name) {
        _name = name;
    }

    public String getName() {
        return _name;
    }


}
