// $Id: QuotaMgrCheckQuotaMessage.java,v 1.1 2006-08-12 21:07:48 patrick Exp $

package diskCacheV111.vehicles;

//Base class for messages to QuotaManager


public class QuotaMgrCheckQuotaMessage extends Message {

    private final String _storageClass;
    private long   _hardQuota;
    private long   _softQuota;
    private long   _spaceUsed;

    private static final long serialVersionUID = 2092295899703859605L;

    public QuotaMgrCheckQuotaMessage(String storageClass){
	_storageClass = storageClass;
    }
    public String getStorageClass(){ return _storageClass ; }
    public void setQuotas( long softQuota , long hardQuota , long spaceUsed ){
       _softQuota = softQuota ;
       _hardQuota = hardQuota ;
       _spaceUsed = spaceUsed ;
    }
    public long getSoftQuota(){ return _softQuota ; }
    public long getHardQuota(){ return _hardQuota ; }
    public long getSpaceUsed(){ return _spaceUsed ; }
    public boolean isSoftQuotaExceeded(){ return _spaceUsed > _softQuota ; }
    public boolean isHardQuotaExceeded(){ return _spaceUsed > _hardQuota ; }
    public String toString(){
        int i = getReturnCode() ;
        if( i != 0 ){
           return "Problem "+i+" : "+getErrorObject().toString();
        }else{
           return "StorageClass="+_storageClass+";Hard="+_hardQuota+";Soft="+_softQuota+";Used="+_spaceUsed ;
        }
    }
}



