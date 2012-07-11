package dmg.security.cipher ;
import java.util.Hashtable ;
import java.util.Enumeration ;
import java.io.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class EncryptionKeyContainer {

    private Hashtable _publicList  = new Hashtable() ;
    private Hashtable _sharedList  = new Hashtable() ;
    private Hashtable _privateList = new Hashtable() ;
    
    public synchronized void addKey( EncryptionKey key ){
    
        String [] list = key.getDomainList() ;
        String    mode = key.getKeyMode() ;
        Hashtable hash = mode.equals("public")  ? _publicList  :
                         mode.equals("private") ? _privateList :
                         mode.equals("shared" ) ? _sharedList  : null ;
        if( hash == null ) {
            return;
        }
        
        for( int i = 0 ; i < list.length ; i++ ) {
            hash.put(list[i], key);
        }
    }
    public synchronized void readInputStream( EncryptionKeyInputStream stream )
           throws IOException {
       EncryptionKey key ;    
       while( ( key = stream.readEncryptionKey() ) != null ) {
           addKey(key);
       }
          
    }
    public synchronized EncryptionKey get( String mode , String name )
           throws EncryptionKeyNotFoundException   {
           
        EncryptionKey key = null ;
        Hashtable hash = mode.equals("public")  ? _publicList  :
                         mode.equals("private") ? _privateList :
                         mode.equals("shared" ) ? _sharedList  : null ;
        
        if( hash == null ){
          if((key = (EncryptionKey)_publicList.get(name)) == null ){
            if((key = (EncryptionKey)_privateList.get(name)) == null ){
              if((key = (EncryptionKey)_sharedList.get(name)) != null ) {
                  return key;
              }
            }          
          }
        }else{
          key = (EncryptionKey)hash.get( name ) ;
        }                 
        if( key == null ) {
            throw new EncryptionKeyNotFoundException(name + " : not found");
        }
          
        return key ;
    }
//    public synchronized Enumeration  keyNames(){ return keys() ; }
    public void update() {}


    public static void main( String [] args ){
      if( args.length != 1 ){
         System.out.println( "USAGE : ... <keyfilename> " ) ;
         System.exit(4) ;
      }
      try{
         MixedKeyInputStream in = new MixedKeyInputStream( 
                                  new FileInputStream( args[0] ) ) ;
         EncryptionKeyContainer container = 
                    new EncryptionKeyContainer() ;
         
 //        container.readEncryptionKey( in ) ;
         
      
      }catch(IOException e ){
        System.err.println( " Exception : " + e ) ;
        System.exit(4);
      }
   
   }

}
