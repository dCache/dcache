package diskCacheV111.namespace.provider;

import java.util.StringTokenizer;
import diskCacheV111.util.CacheException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.util.*;


public class ChecksumCollection {

    private static  final String CHECKSUM_DELIMITER=",";
    private boolean useStringKey ;

    public ChecksumCollection(String rep) throws CacheException {
        this(rep, false);
    }

    public ChecksumCollection(String rep, boolean useStringKey)
        throws CacheException
    {
      this.useStringKey = useStringKey;
      if ( rep != null ) parseRep(rep);
    }

    private void parseRep(String rep) throws CacheException {
      StringTokenizer st = new StringTokenizer(rep,CHECKSUM_DELIMITER);

      while(st.hasMoreTokens() ){
          String currentValue = st.nextToken();

          int checksumValuePos = currentValue.indexOf(":");
          if ( checksumValuePos < 0 )
             throw new CacheException(CacheException.ATTRIBUTE_FORMAT_ERROR,"Checksum stored in the wrong format "+currentValue);

          if ( checksumValuePos == currentValue.length() )
             throw new CacheException(CacheException.ATTRIBUTE_FORMAT_ERROR,"Checksum stored in the wrong format "+currentValue);
         String stringKey = currentValue.substring(0,checksumValuePos);
         int key ;
         if(useStringKey) {
             key = typeStoN(stringKey);
         } else {
            key = Integer.parseInt(stringKey);
         }
         String value =currentValue.substring(checksumValuePos+1);
         put(key,value);
      }
   }

   public void add(ChecksumCollection coll){
      _map.putAll(coll._map);
   }

   public String get(int checksumType){ return _map.get(new Integer(checksumType)); }
   public void put(int checksumType,String value){ _map.put(new Integer(checksumType),value); }

   public int[] types() {
      if ( _map.isEmpty() )
         return null;

      int [] result =  new int[_map.size()];

      int index = 0;
      for(Iterator<Map.Entry<Integer,String>> i = _map.entrySet().iterator();
         i.hasNext(); ){
         result[index] = i.next().getKey().intValue();
         ++index;
      }
      return result;
   }

   public String serialize(){
     StringBuffer result = new StringBuffer();
     int mod = 0;
     for(Iterator<Map.Entry<Integer,String>> i = _map.entrySet().iterator();
         i.hasNext(); ){

       Map.Entry<Integer,String> el = i.next();
       String value = el.getValue();
       if ( value != null ){
          String key;
          if(useStringKey) {
              key = typeNtoS(el.getKey());
          } else {
              key = el.getKey().toString();
          }
          result.append(key).append(":").append(el.getValue()).append(CHECKSUM_DELIMITER);
          mod = CHECKSUM_DELIMITER.length();
       }
     }
     return result.substring(0,result.length()-mod);
   }

   public Set<Checksum> getChecksums(){
     Set<Checksum> checksums = new HashSet<Checksum>();
     int mod = 0;
     for(Iterator<Map.Entry<Integer,String>> i = _map.entrySet().iterator();
         i.hasNext(); ){

       Map.Entry<Integer,String> el = i.next();
       String value = el.getValue();
      ChecksumType cksmtype = ChecksumType.getChecksumType(el.getKey());
      Checksum chksm = new Checksum(cksmtype,value);
      checksums.add(chksm);
     }

     return checksums;
   }


   private static int typeStoN(String typeName) throws CacheException {
      try {
       return ChecksumFactory.mapStringTypeToId(typeName);
      } catch ( NoSuchAlgorithmException ex ){
        throw new CacheException(CacheException.ATTRIBUTE_FORMAT_ERROR,"Checksum type is not supported:"+typeName);
      }
   }

   private static String typeNtoS(int type) {
      try {
       return ChecksumFactory.mapIdTypeToString(type);
      } catch ( NoSuchAlgorithmException ex ){
        throw new IllegalArgumentException("Checksum type is not supported:"+Integer.toString(type));
      }
   }


   private Map<Integer,String> _map = new HashMap<Integer,String>();


  public static final Set<Checksum> extractChecksums(StorageInfo storageInfo) throws Exception{
      Map<Integer,String> cksumMap = new HashMap();
      String ckey = storageInfo.getKey("flag-c");
      String uckey=storageInfo.getKey("flag-uc");
      ChecksumCollection collection = new ChecksumCollection(ckey);
      ChecksumCollection uccollection = new ChecksumCollection(uckey,true);
      collection.add(uccollection);

      return collection.getChecksums();
  }

}
