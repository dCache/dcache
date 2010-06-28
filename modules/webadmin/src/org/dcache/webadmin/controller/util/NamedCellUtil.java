package org.dcache.webadmin.controller.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.NamedCell;

/**
 * Utility for handling the NamedCells (well known cells) in the
 * controlling layer
 * @author jans
 */
public class NamedCellUtil {

    public static Map<String, NamedCell> createCellMap(Set<NamedCell> namedCells) {
        Map<String, NamedCell> cells = new HashMap();
        for (NamedCell currentNamedCell : namedCells) {
            cells.put(currentNamedCell.getCellName(), currentNamedCell);
        }
        return cells;
    }
}
