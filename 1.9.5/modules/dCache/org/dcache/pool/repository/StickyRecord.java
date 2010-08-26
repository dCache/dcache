package org.dcache.pool.repository;

import java.io.Serializable;

public class StickyRecord implements Serializable {

    static final long serialVersionUID = 8235126040387514086L;
	
	private final String _owner;
	private final long   _expire;	
	
	public StickyRecord(String owner, long expire) {
		_owner = owner;
		_expire = expire;
	}	
	
	public boolean isValid() {
		return isValidAt( System.currentTimeMillis() );
	}
	
	public boolean isValidAt(long time) {
		return _expire == -1 || _expire > time;
	}
	
	public long expire() {
		return _expire;
	}
	
	public String owner() {
		return _owner;
	}

	@Override
	public boolean equals(Object obj) {
		
		if ( ! (obj instanceof StickyRecord) ) return false;
		
		
		return ((StickyRecord)obj).owner().equals(_owner) && ((StickyRecord)obj).expire() == _expire;
	}


	@Override
	public int hashCode() {		
		return _owner.hashCode();
	}


	@Override
	public String toString() {	
		return _owner + ":" + _expire;
	}
		
}
