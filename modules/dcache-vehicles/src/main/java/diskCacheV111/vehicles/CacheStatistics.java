//$Id: CacheStatistics.java,v 1.5 2007-05-24 13:51:05 tigran Exp $

package diskCacheV111.vehicles;

import java.io.Serializable;
import java.util.StringTokenizer;

public class CacheStatistics implements Serializable, Comparable<CacheStatistics> {
    private int   _version       = 1;
    private int   _totalAccesses;
    private long  _accessTime;
    private float _score;
    private float _halfLife;
    private static final double __logOneHalf = Math.log(0.5);

    private static final long serialVersionUID = 3951762169312269595L;

    public CacheStatistics(int totalAccesses,
                           long accessTime,
                           float score,
                           float halfLife       ){

	_totalAccesses = totalAccesses;
	_accessTime    = accessTime;
	_score         = score;
	_halfLife      = halfLife;
    }

    public CacheStatistics(){
	this(0,(long)0,(float)0,(float)0);
    }

    public CacheStatistics(String pnfsString) {
	StringTokenizer st = new StringTokenizer(pnfsString,",");

	try {
	    _version = Integer.parseInt(st.nextToken());
	} catch (Exception e){
	    throw new IllegalArgumentException(pnfsString);
	}

	if (_version > 2){
	    throw new
            IllegalArgumentException("statistics version mismatch, got "
                                      +_version+" expected <= 2");
	}

	if (st.countTokens() != 4){
	    throw new
            IllegalArgumentException(pnfsString);
	}

	try {
	    _totalAccesses = Integer.parseInt(st.nextToken());
	    _accessTime    = Long.parseLong(st.nextToken());
	    _score         = new Float(st.nextToken());
	    _halfLife      = new Float(st.nextToken());
	} catch (Exception e){
	    throw new IllegalArgumentException(pnfsString);
	}
    }

    public CacheStatistics(int version, int totalAccesses, long accessTime,
			   float score, float halfLife){
	_version       = version;
	_totalAccesses = totalAccesses;
	_accessTime    = accessTime;
	_score         = score;
	_halfLife      = halfLife;
    }

    public int getTotalAccesses(){
	return _totalAccesses;
    }

    public void setTotalAccesses(int totalAccesses){
	_totalAccesses = totalAccesses;
    }

    public long getAccessTime(){
	return _accessTime;
    }

    public void setAccessTime(long accessTime){
	_accessTime = accessTime;
    }

    public float getScore(){
	return _score;
    }

    public void setScore(float score){
	_score = score;
    }

    public float getHalfLife(){
	return _halfLife;
    }

    public void setHalfLife(float halfLife){
	_halfLife = halfLife;
    }

    public void setVersion( int version ){
      _version = version ;
    }
    public int getVersion(){ return _version ; }

    public String toPnfsString(){
	return _version + "," + _totalAccesses + "," + _accessTime + "," + _score + ',' + _halfLife;
    }

    public String toString(){
	return this.toPnfsString();
    }

    public double age(float then, float now){
	if (_halfLife==0){
	    return 0;
	} else {
	    return Math.exp(__logOneHalf*(now-then)/_halfLife);
	}
    }

    public float currentValue(){
        return currentValue(System.currentTimeMillis());
    }

    public float currentValue(long now){
        if (_accessTime==0){
            return 0;
	} else {
	    return (float)(_score * age(_accessTime, now));
	}
    }

    public void markAccessed(long accessTime){
	++_totalAccesses;
	if (_accessTime==0){
	    _score = 1;
	} else {
	    _score = (float)(_score * age(_accessTime, accessTime) + 1);
	    _accessTime = accessTime;
	}
    }

    @Override
    public int compareTo(CacheStatistics other ){
        //
        //XXX this needs to use the half-life algorithm
        //
	if (_totalAccesses == other.getTotalAccesses()){
            return _accessTime>other.getAccessTime()?-1:_accessTime==other.getAccessTime()?0:1;
//	    return (new Long(_accessTime)).compareTo(new Long(other.getAccessTime()));
	} else {
            return _totalAccesses>other.getTotalAccesses()?
                      -1:_totalAccesses==other.getTotalAccesses()?0:1;
//	    return (new Integer(_totalAccesses)).compareTo(new Integer(other.getTotalAccesses()));
	}
    }

    public static void main(String[] args){
	CacheStatistics s = new CacheStatistics(args[0]);
	System.out.println(s.toPnfsString());
    }
}




