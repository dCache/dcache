package org.dcache.pool.repository.v3.entry.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dcache.pool.repository.StickyRecord;

/**
 *
 * file sticky status.
 * @since 1.7.1
 *
 */

public class Sticky {

	private final Map<String,StickyRecord> _records = new HashMap<String,StickyRecord>();


	public boolean isSticky() {

		boolean isSticky = false;

		if( !_records.isEmpty() ) {
			long now = System.currentTimeMillis();

			for( StickyRecord stickyRecord: _records.values() ) {
				if( stickyRecord.isValidAt(now) ) {
					// even if only one is valid it's STICKY
					isSticky = true;
					break;
				}
			}
		}
		return isSticky;
	}

	public boolean isSet() {
		return isSticky();
	}

	public boolean addRecord(String owner, long expire) {
		boolean isAdded = false;
		if( expire == -1 || expire > System.currentTimeMillis() ) {
			_records.put(owner, new StickyRecord(owner, expire) );
			isAdded = true;
		}

		return isAdded;
	}

	public boolean removeRecord(String owner) {
		return _records.remove(owner) != null ;
	}

	public String stringValue() {

		StringBuilder sb = new StringBuilder();

		long now = System.currentTimeMillis();

		for( StickyRecord record: _records.values() ) {
			if( record.isValidAt(now) ) {
				sb.append("sticky:").append(record.owner()).append(":").append(record.expire()).append("\n");
			}
		}

		return sb.toString();
	}

	public List<StickyRecord> records() {
		return new ArrayList<StickyRecord>(_records.values());
	}
}
