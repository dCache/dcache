// $Id: AttributeChecksumBridge.java,v 1.3 2007-09-21 15:09:59 tigran Exp $

package diskCacheV111.namespace.provider;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;
import  diskCacheV111.namespace.provider.*;
import  diskCacheV111.namespace.NameSpaceProvider;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

import  java.io.* ;
import  java.util.*;
import  diskCacheV111.util.ChecksumFactory;
import java.security.NoSuchAlgorithmException;


class MyFakeNameSpaceProvider implements NameSpaceProvider {

    private Map<String,Object> _map = new HashMap<String,Object>();

    public void setFileMetaData(PnfsId pnfsId, FileMetaData metaData){}
    public FileMetaData getFileMetaData(PnfsId pnfsId) throws Exception { return null; }


    public PnfsId createEntry(String name, FileMetaData metaData, boolean checksumType) throws Exception { return null; }
    public void deleteEntry( PnfsId pnfsId) throws Exception {}
    public void deleteEntry( String path) throws Exception {}
    public void renameEntry( PnfsId pnfsId, String newName) throws Exception {}

    public String pnfsidToPath( PnfsId pnfsId) throws Exception { return null; }
    public PnfsId pathToPnfsid( String path, boolean followLinks) throws Exception { return null; }

    public String[] getFileAttributeList(PnfsId pnfsId) { return null; }

    public Object getFileAttribute( PnfsId pnfsId, String attribute) { 
      Object result = _map.get(attribute); 
      System.out.println("Retrieved atttributed "+attribute+" result "+result); 
      return result;
    }

    public void removeFileAttribute( PnfsId pnfsId, String attribute) {  }

    public void setFileAttribute( PnfsId pnfsId, String attribute, Object data) { 
      System.out.println("Setting attribute "+attribute+" to "+data); 
      _map.put(attribute,data); 
    }

    public void setLevelData( PnfsId pnfsId, Map<Integer, String> levelData) throws Exception { }

    public void addChecksum(PnfsId pnfsId, int type, String value) throws Exception {}
    public String getChecksum(PnfsId pnfsId, int type) throws Exception { return null; }
    public void removeChecksum(PnfsId pnfsId, int type) throws Exception {}
   public int[] listChecksumTypes(PnfsId pnfsId) throws Exception { return null;}

}

class ChecksumCollection {
    
    private static  final String CHECKSUM_DELIMITER=",";
    private boolean useStringKey ;
    
    public ChecksumCollection(String rep) throws Exception {
        this(rep, false);
    }

    public ChecksumCollection(String rep, boolean useStringKey) throws Exception {
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

};

public class AttributeChecksumBridge {

   private static  final String CHECKSUM_COLLECTION_FLAG="uc";

   private NameSpaceProvider _nameSpaceProvider;

   private static void print(String atr){
      System.out.println(atr);
   }

   public static void main( String [] args ) throws Exception {

      print("Getting checksum type MD5");

      AttributeChecksumBridge mgr = new AttributeChecksumBridge(new MyFakeNameSpaceProvider());
      print(mgr.getChecksum(null,Checksum.MD5));

      mgr.setChecksum(null,"MD5 value",Checksum.MD5);

      print("Newly set checksum "+mgr.getChecksum(null,Checksum.MD5));

      mgr.setChecksum(null,"Adler value",Checksum.ADLER32);

      print("Newly set adler checksum "+mgr.getChecksum(null,Checksum.ADLER32));

      print("Old MD5 checksum "+mgr.getChecksum(null,Checksum.MD5));

      mgr.setChecksum(null,"Other checksum value",3);

      print("Newly set outher checksum "+mgr.getChecksum(null,3));

      print("Old Adler checksum "+mgr.getChecksum(null,Checksum.ADLER32));

      print("Clearing checksum value");
      mgr.removeChecksum(null,Checksum.MD5);
      print("MD5 should be now null "+mgr.getChecksum(null,Checksum.MD5));

     AttributeChecksumBridge mgr1 = new AttributeChecksumBridge(new MyFakeNameSpaceProvider());

     mgr1.setChecksum(null,"MD5 value",Checksum.MD5);
     mgr1.setChecksum(null,"MD4 value",Checksum.MD4);
     int tps[] = mgr1.types(null);
     for ( int i = 0; i < tps.length; ++i)
        print(Integer.toString(tps[i]));
   }

   public AttributeChecksumBridge(NameSpaceProvider nameSpaceProvider)
   {
      _nameSpaceProvider = nameSpaceProvider;
   }

   public String getChecksum(PnfsId pnfsId,int checksumType) throws Exception {
 
      if ( checksumType == Checksum.MD5 || checksumType == Checksum.ADLER32 ){
        // look into "c" flag 
        String flagValue = (String)_nameSpaceProvider.getFileAttribute(pnfsId, "c");
        ChecksumCollection collection = new ChecksumCollection(flagValue);
        String candidate = collection.get(checksumType);
        if ( candidate != null )
          return candidate;
      } 

      return new ChecksumCollection((String)_nameSpaceProvider.getFileAttribute(pnfsId, CHECKSUM_COLLECTION_FLAG),true).get(checksumType);
   }

   public void setChecksum(PnfsId pnfsId,String value,int checksumType) throws Exception {

      // alder32 is always stored where everyone is expecting it to - using c flag
      // the other types are packed into list which serizalized value is managed under CHECKSUM_COLLECTION_FLAG
      if ( checksumType == Checksum.ADLER32 ){
        // look into "c" flag
        String flagValue = (String)_nameSpaceProvider.getFileAttribute(pnfsId, "c");
        ChecksumCollection collection = new ChecksumCollection(flagValue);

        if ( flagValue != null ){

          // if its a same checksumType - replace. otherwise (i.e. legacy stored MD5) - fall back to using CHECKSUM_COLLECTION_FLAG
          if ( collection.get(checksumType) != null ){
             collection.put(checksumType,value);
             setFileAttribute(pnfsId, "c",collection.serialize());
             return;
          }
        } else {
          collection.put(checksumType,value);
          setFileAttribute(pnfsId, "c",collection.serialize());
          return;
        }
      }

      ChecksumCollection collection = 
          new ChecksumCollection((String)_nameSpaceProvider.getFileAttribute(pnfsId, CHECKSUM_COLLECTION_FLAG),true);

      collection.put(checksumType,value);
      String flagValue = collection.serialize();
      setFileAttribute(pnfsId, CHECKSUM_COLLECTION_FLAG, flagValue);
   }

   public void removeChecksum(PnfsId pnfsId, int type) throws Exception {
     setChecksum(pnfsId,null,type);
   }

   public int[] types(PnfsId pnfsId) throws Exception {
     String flagValue = (String)_nameSpaceProvider.getFileAttribute(pnfsId, "c");
     ChecksumCollection collectionA = new ChecksumCollection(flagValue);

     flagValue = (String)_nameSpaceProvider.getFileAttribute(pnfsId, CHECKSUM_COLLECTION_FLAG);
     ChecksumCollection collectionB = new ChecksumCollection(flagValue,true);

     collectionA.add(collectionB);

     return collectionA.types();
   }

   private void setFileAttribute(PnfsId pnfsId, String attrName, String value ) {
         if(value != null && value.length() >0) {
            _nameSpaceProvider.setFileAttribute(pnfsId, attrName, value);
         } else {
             _nameSpaceProvider.removeFileAttribute(pnfsId,attrName);
         }
   }

}

