package org.dcache.pool.repository;

import java.io.Serializable;
import java.text.DateFormat;

public class StickyRecord implements Serializable {

    private static final long serialVersionUID = 8235126040387514086L;

    private static final DateFormat df = DateFormat.getInstance();
    public static final long NON_EXPIRING = -1;

	private final String _owner;
	private final long   _expire;


	public StickyRecord(String owner, long expire)
	{
		_owner = owner;
		_expire = expire;
	}

	public boolean isValid()
	{
		return isValidAt(System.currentTimeMillis());
	}

	public boolean isValidAt(long time)
	{
		return isNonExpiring() || _expire > time;
	}

	public boolean isNonExpiring()
	{
	    return _expire == NON_EXPIRING;
	}

	public long expire()
	{
		return _expire;
	}

	public String owner()
	{
		return _owner;
	}

	@Override
	public boolean equals(Object obj)
	{
		if( !(obj instanceof StickyRecord)) {
		    return false;
		}

		StickyRecord other = (StickyRecord) obj;

		return other.owner().equals(_owner) && other.expire() == _expire;
	}


	@Override
	public int hashCode()
	{
		return _owner.hashCode();
	}


	@Override
	public synchronized String toString()
	{
	    String expiryLabel = isNonExpiring() ? "never expires" : "expires " + df.format(_expire);
		return _owner + " : " + expiryLabel;
	}
}
