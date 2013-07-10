package org.dcache.webadmin.controller.util;

import java.util.List;
import java.util.Map;

/**
 * Utility for handling the NamedCells (well known cells) in the
 * controlling layer
 * @author jans
 */
public class NamedCellUtil {

    public static String findDomainOfUniqueCell(Map<String, List<String>> domains,
            String cell) {
        String domain = "";
        for (Map.Entry<String, List<String>> entry : domains.entrySet()) {
            if (entry.getValue().contains(cell)) {
                domain = entry.getKey();
                break;
            }
        }
        return domain;
    }
}
