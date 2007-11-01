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
    
    public int hashCode() {
        return this.toString().hashCode();
    }
	
	public int session() { return _sessionID; }
	public byte[] challenge() { return _challenge; }
    
    public String toString() {
        return "[" + _sessionID + ":" + new String(_challenge) + "]";
    }
    
    
    public static void main(String[] args) {
        
        int id = 17;
        byte[] challenge1 = "hello world".getBytes();
        byte[] challenge2 = "hello world".getBytes();
        
        DCapProrocolChallenge c1 = new DCapProrocolChallenge(id, challenge1);
        DCapProrocolChallenge c2 = new DCapProrocolChallenge(id, challenge2);
        
        System.out.println(" c1 == c2 ? : " + c1.equals(c2));
        System.out.println(" c1.hashCode = " + c1.hashCode() + "  c2.hashCode = " + c2.hashCode());
    }
    
}
