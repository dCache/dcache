/*
 * $Id: DCapProrocolChallenge.java,v 1.1 2006-07-18 09:08:22 tigran Exp $
 */

package diskCacheV111.util;

public class DCapProrocolChallenge {

	private int _sessionID;
	private byte[] _challenge;

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
    public boolean equals(Object otherChallenge) {

		boolean equals = false;

		if( otherChallenge instanceof  DCapProrocolChallenge ) {
			if(_sessionID == ((DCapProrocolChallenge)otherChallenge).session() ) {
				if( _challenge.length == ((DCapProrocolChallenge)otherChallenge).challenge().length ) {
					byte[] challengeToCompare = ((DCapProrocolChallenge)otherChallenge).challenge();
					for(int i = 0; i < _challenge.length; i++) {
						if(_challenge[i] != challengeToCompare[i] ) {
							return false;
						}
					}
					equals = true;
				}
			}
		}

		return equals;

	}

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

	public int session() { return _sessionID; }
	public byte[] challenge() { return _challenge; }

    @Override
    public String toString() {
        return "[" + _sessionID + ":" + new String(_challenge) + "]";
    }

}
