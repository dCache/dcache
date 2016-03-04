/*
 * $Id: DCapProrocolChallenge.java,v 1.1 2006-07-18 09:08:22 tigran Exp $
 */

package diskCacheV111.util;

import java.util.Arrays;

public class DCapProrocolChallenge {

	private final int _sessionID;
	private final byte[] _challenge;

	public DCapProrocolChallenge(int session, byte[] challenge) {
		_sessionID = session;
		_challenge = challenge;
	}

    /*
     *
     * If two objects are equal according to the equals(Object)  method,
     * then calling the hashCode method on each of the two objects must
     * produce the same integer result.
     *
     */

    @Override
    public boolean equals(Object other) {

        if( other == this) {
            return true;
        }

        if ( !(other instanceof DCapProrocolChallenge)) {
            return false;
        }

        DCapProrocolChallenge otherChallenge = (DCapProrocolChallenge) other;

        return _sessionID == otherChallenge.session() &&
            Arrays.equals(_challenge, otherChallenge.challenge());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(_challenge);
    }

	public int session() { return _sessionID; }
	public byte[] challenge() { return _challenge; }

    @Override
    public String toString() {
        return "[" + _sessionID + ":" + new String(_challenge) + "]";
    }

}
