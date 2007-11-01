package org.dcache.pool.repository.v3.entry.state;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.dcache.pool.repository.StickyRecord;

/**
 * 
 * file sticky status. 
 * @since 1.7.1
 *
 */

public class Sticky {

	private final Set<StickyRecord> _records = new HashSet<StickyRecord>();
	
	
	public boolean isSticky() {
		
		boolean isSticky = false;
		
		if( !_records.isEmpty() ) {
			long now = System.currentTimeMillis();
			
			for( StickyRecord stickyRecord: _records ) {
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
			_records.add( new StickyRecord(owner, expire) );
			isAdded = true;
		}
		
		return isAdded;
	}
	
	public boolean removeRecord(String owner, long expire) {
		return _records.remove( new StickyRecord(owner, expire) );
	}
		
	public String stringValue() {
		
		StringBuilder sb = new StringBuilder();
		
		long now = System.currentTimeMillis();
		
		for( StickyRecord record: _records ) {			
			if( record.isValidAt(now) ) {
				sb.append("sticky:").append(record.owner()).append(":").append(record.expire());
			}
		}		
		
		return sb.toString();
	}
	
	public List<StickyRecord> records() {		
		return new ArrayList<StickyRecord>(_records);		
	}	
}
