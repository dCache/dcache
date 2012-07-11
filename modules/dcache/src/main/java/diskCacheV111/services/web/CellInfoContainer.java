// $Id: CellInfoContainer.java,v 1.1 2006-06-08 15:23:27 patrick Exp $Cg

package diskCacheV111.services.web;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

public class CellInfoContainer
{
    private final Map<String,Map<String,Map<String,Object>>> _poolHash
        = new HashMap<String,Map<String,Map<String,Object>>>();
    private final Map<String,PatternEntry> _patternHash
        = new HashMap<String,PatternEntry>();
    private final Map<String,Map<String,Map<String,Object>>> _poolGroupClassHash
        = new HashMap<String,Map<String,Map<String,Object>>>();

    public synchronized void addInfo(String poolName, Object payload)
    {
        Map<String,Map<String,Object>> link = _poolHash.get(poolName);
        if (link != null) {
            for (Map<String,Object> table: link.values()) {
                table.put(poolName, payload);
            }
        }

        for (PatternEntry patternEntry : _patternHash.values()) {
            if (patternEntry.pattern.matcher(poolName).matches()) {
                for (Map<String,Object> table: patternEntry.linkMap.values()) {
                    table.put(poolName, payload);
                }
            }
        }
    }

    public synchronized void addPool(String groupClass, String group, String poolName)
    {
        Map<String,Map<String,Object>> poolGroupMap =
            _poolGroupClassHash.get(groupClass);
        if (poolGroupMap == null) {
            poolGroupMap =
                new HashMap<String,Map<String,Object>>();
            _poolGroupClassHash.put(groupClass, poolGroupMap);
        }

        Map<String,Object> table = poolGroupMap.get(group);
        if (table == null) {
            table = new HashMap<String,Object>();
            poolGroupMap.put(group, table);
        }

        Map<String,Map<String,Object>> link = _poolHash.get(poolName);
        if (link == null) {
            link = new HashMap<String,Map<String,Object>>();
            _poolHash.put(poolName, link);
        }

        link.put(groupClass + ":" + group, table);
    }

    public synchronized void removePool(String groupClass, String group, String poolName)
        throws NoSuchElementException, IllegalStateException
    {
        Map<String,Map<String,Object>> poolGroupMap =
            _poolGroupClassHash.get(groupClass);
        if (poolGroupMap == null) {
            throw new
                    NoSuchElementException("groupClass not found : "
                    + groupClass);
        }
        Map<String,Object> tableMap = poolGroupMap.get(group);
        if (tableMap == null) {
            throw new
                    NoSuchElementException("group not found : " + group);
        }
        //
        //
        // now get the table map from the poolHash side
        //
        Map<String,Map<String,Object>> link = _poolHash.get(poolName);
        if (link == null) {
            throw new
                    NoSuchElementException("pool not found : " + poolName);
        }

        tableMap = link.remove(groupClass + ":" + group);
        if (tableMap == null) {
            throw new
                    IllegalStateException("not found in link map : "
                    + groupClass + ":" + group);
        }
        //
        // here we should check if both table maps are the same. But
        // we wouldn't know what to do if not.
        //
        // clear the possible content
        //
        tableMap.remove(poolName);
    }
    //
    //   NOT FINISHED YET
    //
    public synchronized void removePoolGroup(String className, String groupName)
    {
        //
        // first remove pool group from poolGroupClass hash
        //
        Map<String,Map<String,Object>> groupMap =
            _poolGroupClassHash.get(className);
        if (groupMap == null) {
            throw new
                    NoSuchElementException("not found : " + className);
        }

        Map<String,Object> tableMap = groupMap.remove(groupName);
        if (tableMap == null) {
            throw new
                    NoSuchElementException("not found : " + groupName);
        }

        String d = className + ":" + groupName;

        for (Map.Entry<String,Map<String,Map<String,Object>>> entry:
                 _poolHash.entrySet()) {
            String poolName = entry.getKey();
            Map<String,Map<String,Object>> link = entry.getValue();
            for (Map.Entry<String,Map<String,Object>> domain:
                     link.entrySet()) {
                String domainName = domain.getKey();
                Map<String,Object> table = domain.getValue();

                // FIXME: Is something supposed to happen here?
            }
        }
    }

    private static class PatternEntry
    {
        private final Map<String,Map<String,Object>> linkMap
            = new HashMap<String,Map<String,Object>>();
        private final Pattern pattern;

        private PatternEntry(Pattern pattern)
        {
            this.pattern = pattern;
        }

        public String toString()
        {
            return pattern.pattern() + " " + linkMap.toString();
        }
    }

    public synchronized void addPattern(String groupClass, String group, String patternName, String pattern)
    {
        Map<String,Map<String,Object>> poolGroupMap =
            _poolGroupClassHash.get(groupClass);
        if (poolGroupMap == null) {
            poolGroupMap = new HashMap<String,Map<String,Object>>();
            _poolGroupClassHash.put(groupClass, poolGroupMap);
        }

        Map<String,Object> table = poolGroupMap.get(group);
        if (table == null) {
            table = new HashMap<String,Object>();
            poolGroupMap.put(group, table);
        }

        PatternEntry patternEntry = _patternHash.get(patternName);
        if (patternEntry == null) {
            if (pattern == null) {
                throw new
                        IllegalArgumentException("patterName is new, so we need pattern");
            }

            patternEntry = new PatternEntry(Pattern.compile(pattern));
            _patternHash.put(patternName, patternEntry);
        } else {
            if (pattern != null) {
                if (! patternEntry.pattern.pattern().equals(pattern)) {
                    throw new
                            IllegalArgumentException("Conflict in pattern (name in use with different pattern)");
                }
            }
        }

        patternEntry.linkMap.put(groupClass + ":" + group, table);
    }

    public synchronized void removePattern(String groupClass, String group, String patternName)
    {
        Map<String,Map<String,Object>> poolGroupMap =
            _poolGroupClassHash.get(groupClass);
        if (poolGroupMap == null) {
            throw new
                    NoSuchElementException("groupClass not found : "
                    + groupClass);
        }

        Map<String,Object> tableMap = poolGroupMap.get(group);
        if (tableMap == null) {
            throw new
                    NoSuchElementException("group not found : " + group);
        }
        //
        //
        // now get the table map from the poolHash side
        //
        PatternEntry patternEntry = _patternHash.get(patternName);
        if (patternEntry == null) {
            throw new
                    NoSuchElementException("patternName not found : "
                    + patternName);
        }

        Map<String,Map<String,Object>> link = patternEntry.linkMap;

        tableMap = link.remove(groupClass + ":" + group);
        if (tableMap == null) {
            throw new
                    IllegalStateException("not found in link map : "
                    + groupClass + ":" + group);
        }

        // if (link.size() == 0)_patternHash.remove(patternName);
        //
        // here we should check if both table maps are the same. But we wouldn't know
        // what to do if not.
        //
        // clear the possible content
        //
        List<String> toBeRemoved = new ArrayList<String>();
        for (String poolName : tableMap.keySet()) {
            if (patternEntry.pattern.matcher(poolName).matches()) {
                toBeRemoved.add(poolName);
            }
        }
        for (String poolName : toBeRemoved) {
            tableMap.remove(poolName);
        }
    }

    public synchronized String getInfo()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String,Map<String,Map<String,Object>>> entry:
                 _poolGroupClassHash.entrySet()) {
            String className = entry.getKey();
            Map<String,Map<String,Object>> groupMap = entry.getValue();

            sb.append("Class : ").append(className).append("\n");

            for (Map.Entry<String,Map<String,Object>> groupEntry:
                     groupMap.entrySet()) {
                String groupName = groupEntry.getKey();
                Map<String,Object> tableMap  = groupEntry.getValue();

                sb.append("   Group : ").append(groupName).append("\n");

                printTable(sb, "            ", tableMap);
            }
        }

        sb.append("PoolHash :\n");
        for (Map.Entry<String,Map<String,Map<String,Object>>> entry:
                 _poolHash.entrySet()) {
            String poolName = entry.getKey();
            Map<String,Map<String,Object>> link = entry.getValue();

            sb.append("  ").append(poolName).append("\n");

            for (Map.Entry<String,Map<String,Object>> domain:
                     link.entrySet()) {
                String domainName = domain.getKey();
                Map<String,Object> table = domain.getValue();

                sb.append("     ").append(domainName).append("\n");

                printTable(sb, "           ", table);
            }
        }
        sb.append("Pattern List :\n");
        for (Map.Entry<String,PatternEntry> entry:
                 _patternHash.entrySet()) {
            String       patternName  = entry.getKey();
            PatternEntry patternEntry = entry.getValue();
            Pattern      pattern      = patternEntry.pattern;
            Map<String,Map<String,Object>> link = patternEntry.linkMap;

            sb.append("  ").append(patternName).append("(").append(pattern.pattern()).append(")").append("\n");

            for (Map.Entry<String,Map<String,Object>> domain:
                     link.entrySet()) {
                String domainName = domain.getKey();
                Map<String,Object> table = domain.getValue();

                sb.append("     ").append(domainName).append("\n");

                printTable(sb, "           ", table);
            }
        }
        return sb.toString();
    }

    private void printTable(StringBuilder sb, String prefix,
                            Map<String,Object> table)
    {
        for (Map.Entry<String,Object> tableEntry : table.entrySet()) {
            String pn = tableEntry.getKey();
            String tc = tableEntry.getValue().toString();

            sb.append(prefix).append(pn).append(" -> ").append(tc).append("\n");
        }
    }

    public synchronized Map<String,Map<String,Map<String,Object>>>
        createExternalTopologyMap()
    {
        Map<String,Map<String,Map<String,Object>>> allClasses =
            new HashMap<String,Map<String,Map<String,Object>>>();

        for (Map.Entry<String,Map<String,Map<String,Object>>> entry:
                 _poolGroupClassHash.entrySet()) {
            String className = entry.getKey();
            Map<String,Map<String,Object>> groupMap = entry.getValue();

            Map<String,Map<String,Object>> currentClass =
                new HashMap<String,Map<String,Object>>();
            allClasses.put(className, currentClass);

            for (Map.Entry<String,Map<String,Object>> groupEntry:
                     groupMap.entrySet()) {
                String groupName  = groupEntry.getKey();
                Map<String,Object> tableMap = groupEntry.getValue();

                Map<String,Object> currentGroup =
                    new HashMap<String,Object>();
                currentClass.put(groupName, currentGroup);

                for (String poolName: tableMap.keySet()) {
                    currentGroup.put(poolName, null);
                }
            }
        }
        return allClasses;
    }
}

