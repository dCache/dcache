package org.dcache.pool.repository.v3.entry.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Iterator;

import org.dcache.pool.repository.StickyRecord;

/**
 *
 * file sticky status.
 * @since 1.7.1
 *
 */

public class Sticky {

    private final Map<String,StickyRecord> _records = new HashMap<String,StickyRecord>();


    synchronized public boolean isSticky() {
        return !_records.isEmpty();
    }

    public boolean isSet() {
        return isSticky();
    }

    synchronized public boolean addRecord(String owner, long expire, boolean overwrite)
    {
        if (!removeRecord(owner, overwrite ? -1 : expire)) {
            return false;
        }

        if ((expire == -1) || (expire > System.currentTimeMillis())) {
            _records.put(owner, new StickyRecord(owner, expire));
        }
        return true;
    }

    /**
     * Removes all sticky flags owned by <code>owner</code> and not
     * valid at <code>time</code>. No flag is valid at time point -1.
     *
     * Returns true if all flags owned by <code>owner</code> have been
     * removed, false otherwise.
     */
    synchronized private boolean removeRecord(String owner, long time)
    {
        StickyRecord record = _records.get(owner);
        if ((record != null) && (time > -1) && record.isValidAt(time))
            return false;

        _records.remove(owner);
        return true;
    }

    synchronized public String stringValue() {

        StringBuilder sb = new StringBuilder();

        long now = System.currentTimeMillis();

        for( StickyRecord record: _records.values() ) {
            if( record.isValidAt(now) ) {
                sb.append("sticky:").append(record.owner()).append(":").append(record.expire()).append("\n");
            }
        }

        return sb.toString();
    }

    synchronized public List<StickyRecord> records() {
        return new ArrayList<StickyRecord>(_records.values());
    }

    /**
     * Removes expired flags. Returns the list of removed records.
     */
    synchronized public List<StickyRecord> removeExpired()
    {
        List<StickyRecord> removed = new ArrayList();
        long now = System.currentTimeMillis();
        Iterator<StickyRecord> i = _records.values().iterator();
        while (i.hasNext()) {
            StickyRecord record = i.next();
            if (!record.isValidAt(now)) {
                i.remove();
                removed.add(record);
            }
        }
        return removed;
    }

}
