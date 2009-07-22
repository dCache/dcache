// $Id: AttributeChecksumBridge.java,v 1.3 2007-09-21 15:09:59 tigran Exp $

package diskCacheV111.namespace.provider;

import  diskCacheV111.util.*;
import  diskCacheV111.namespace.NameSpaceProvider;

import  java.util.*;
import javax.security.auth.Subject;

public class AttributeChecksumBridge {

   private static  final String CHECKSUM_COLLECTION_FLAG="uc";

   private final NameSpaceProvider _nameSpaceProvider;

   public AttributeChecksumBridge(NameSpaceProvider nameSpaceProvider)
   {
      _nameSpaceProvider = nameSpaceProvider;
   }

    public String getChecksum(Subject subject, PnfsId pnfsId,int checksumType)
       throws CacheException
   {
      if ( checksumType == Checksum.MD5 || checksumType == Checksum.ADLER32 ){
        // look into "c" flag
          String flagValue = (String)_nameSpaceProvider.getFileAttribute(subject, pnfsId, "c");
        ChecksumCollection collection = new ChecksumCollection(flagValue);
        String candidate = collection.get(checksumType);
        if ( candidate != null )
          return candidate;
      }

      return new ChecksumCollection((String)_nameSpaceProvider.getFileAttribute(subject, pnfsId, CHECKSUM_COLLECTION_FLAG),true).get(checksumType);
   }

   public Set<org.dcache.util.Checksum> getChecksums(Subject subject, PnfsId pnfsId)
       throws CacheException
    {
        String flagValue = (String)_nameSpaceProvider.getFileAttribute(subject, pnfsId, "c");
        ChecksumCollection collection = new ChecksumCollection(flagValue);
        flagValue = (String)_nameSpaceProvider.getFileAttribute(subject, pnfsId,
            CHECKSUM_COLLECTION_FLAG);
        ChecksumCollection collection1 =  new ChecksumCollection(flagValue,true);
        collection.add(collection1);
        return collection.getChecksums();
   }


   public void setChecksum(Subject subject, PnfsId pnfsId,String value,int checksumType)
       throws CacheException
    {
      // alder32 is always stored where everyone is expecting it to - using c flag
      // the other types are packed into list which serizalized value is managed under CHECKSUM_COLLECTION_FLAG
      if ( checksumType == Checksum.ADLER32 ){
        // look into "c" flag
          String flagValue = (String)_nameSpaceProvider.getFileAttribute(subject, pnfsId, "c");
        ChecksumCollection collection = new ChecksumCollection(flagValue);

        if (flagValue == null || value == null) {
            collection.put(checksumType,value);
            setFileAttribute(subject, pnfsId, "c", collection.serialize());
            return;
        }

        // if its a same checksumType, then check that it is the same
        // value as before. If it is a different type (i.e. legacy
        // stored MD5), then fall back to using
        // CHECKSUM_COLLECTION_FLAG
        String existingValue = collection.get(checksumType);
        if (existingValue != null) {
            if (!existingValue.equals(value)) {
                throw new CacheException(CacheException.INVALID_ARGS,
                                         "Checksum mismatch");
            }
            return;
        }
      }

      ChecksumCollection collection =
          new ChecksumCollection((String)_nameSpaceProvider.getFileAttribute(subject, pnfsId, CHECKSUM_COLLECTION_FLAG),true);

      String existingValue = collection.get(checksumType);
      if (existingValue != null && value != null) {
          if (!existingValue.equals(value)) {
              throw new CacheException(CacheException.INVALID_ARGS,
                                       "Checksum mismatch");
          }
          return;
      }

      collection.put(checksumType,value);
      String flagValue = collection.serialize();
      setFileAttribute(subject, pnfsId, CHECKSUM_COLLECTION_FLAG, flagValue);
   }

    public void removeChecksum(Subject subject, PnfsId pnfsId, int type)
       throws CacheException
   {
       setChecksum(subject, pnfsId,null,type);
   }

    public int[] types(Subject subject, PnfsId pnfsId) throws CacheException {
     String flagValue = (String)_nameSpaceProvider.getFileAttribute(subject, pnfsId, "c");
     ChecksumCollection collectionA = new ChecksumCollection(flagValue);

     flagValue = (String)_nameSpaceProvider.getFileAttribute(subject, pnfsId, CHECKSUM_COLLECTION_FLAG);
     ChecksumCollection collectionB = new ChecksumCollection(flagValue,true);

     collectionA.add(collectionB);

     return collectionA.types();
   }

    private void setFileAttribute(Subject subject, PnfsId pnfsId, String attrName, String value )
        throws CacheException
    {
        if(value != null && value.length() >0) {
            _nameSpaceProvider.setFileAttribute(subject, pnfsId, attrName, value);
        } else {
            _nameSpaceProvider.removeFileAttribute(subject, pnfsId,attrName);
        }
    }
}

