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
import  diskCacheV111.util.Checksum;


class MyFakeNameSpaceProvider implements NameSpaceProvider {

    private Map<String,Object> _map = new HashMap<String,Object>();

    public void setFileMetaData(PnfsId pnfsId, FileMetaData metaData){}
    public FileMetaData getFileMetaData(PnfsId pnfsId) throws Exception { return null; }


    public PnfsId createEntry(String name, FileMetaData metaData, boolean checksumType) throws Exception { return null; }
    public void deleteEntry( PnfsId pnfsId) throws Exception {}
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
   public ChecksumCollection(String rep) throws Exception {
      if ( rep == null )
         return;

      StringTokenizer st = new StringTokenizer(rep,";");

      while(st.hasMoreTokens() ){
          String currentValue = st.nextToken();

          int checksumValuePos = currentValue.indexOf(":");
          if ( checksumValuePos < 0 )
             throw new CacheException(CacheException.ATTRIBUTE_FORMAT_ERROR,"Checksum stored in the wrong format "+currentValue);

          if ( checksumValuePos == currentValue.length() )
             throw new CacheException(CacheException.ATTRIBUTE_FORMAT_ERROR,"Checksum stored in the wrong format "+currentValue);

         put(Integer.parseInt(currentValue.substring(0,checksumValuePos)),currentValue.substring(checksumValuePos+1));
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
          result.append(el.getKey()).append(":").append(el.getValue()).append(";");
          mod = 1;
       }
     } 
     return result.substring(0,result.length()-mod);
   }

   private Map<Integer,String> _map = new HashMap<Integer,String>();

};

public class AttributeChecksumBridge {

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

      mgr.setChecksum(null,"Other checksum value",44);

      print("Newly set outher checksum "+mgr.getChecksum(null,44));

      print("Old Adler checksum "+mgr.getChecksum(null,Checksum.ADLER32));

      print("Unset checksum value "+mgr.getChecksum(null,100)); 

      print("Clearing checksum value");
      mgr.removeChecksum(null,Checksum.MD5);
      print("MD5 should be now null "+mgr.getChecksum(null,Checksum.MD5));

     AttributeChecksumBridge mgr1 = new AttributeChecksumBridge(new MyFakeNameSpaceProvider());

     mgr1.setChecksum(null,"MD5 value",Checksum.MD5);
     mgr1.setChecksum(null,"MD4 value",3);
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

      return new ChecksumCollection((String)_nameSpaceProvider.getFileAttribute(pnfsId, "c1")).get(checksumType);
   }

   public void setChecksum(PnfsId pnfsId,String value,int checksumType) throws Exception {


      if ( checksumType == Checksum.MD5 || checksumType == Checksum.ADLER32 ){
        // look into "c" flag
        String flagValue = (String)_nameSpaceProvider.getFileAttribute(pnfsId, "c");
        ChecksumCollection collection = new ChecksumCollection(flagValue);

        if ( flagValue != null ){

          // if its a same checksumType - replace otherwise - fall back to using c1
          if ( collection.get(checksumType) != null ){
             collection.put(checksumType,value);
             _nameSpaceProvider.setFileAttribute(pnfsId, "c",collection.serialize());
             return;
          }
        } else {
          // set the value here
          collection.put(checksumType,value);
          _nameSpaceProvider.setFileAttribute(pnfsId, "c",collection.serialize());
          return;
        }
      }

      ChecksumCollection collection = new ChecksumCollection((String)_nameSpaceProvider.getFileAttribute(pnfsId, "c1"));

      collection.put(checksumType,value);
      _nameSpaceProvider.setFileAttribute(pnfsId, "c1", collection.serialize());
   }

   public void removeChecksum(PnfsId pnfsId, int type) throws Exception {
     setChecksum(pnfsId,null,type);
   }

   public int[] types(PnfsId pnfsId) throws Exception {
     String flagValue = (String)_nameSpaceProvider.getFileAttribute(pnfsId, "c");
     ChecksumCollection collectionA = new ChecksumCollection(flagValue);

     flagValue = (String)_nameSpaceProvider.getFileAttribute(pnfsId, "c1");
     ChecksumCollection collectionB = new ChecksumCollection(flagValue);

     collectionA.add(collectionB);

     return collectionA.types();
   }

};

