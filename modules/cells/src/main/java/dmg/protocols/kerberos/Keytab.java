package dmg.protocols.kerberos ;

import java.io.* ;
import java.util.* ;

public class Keytab {
   private Vector _list;
   public class KeytabEntry {
      String _realm ;
      String [] _principals ;
      long      _timestamp ;
      int       _vno ;
      int       _keytype ;
      byte []   _key ;
      public KeytabEntry( String realm , String [] principals ,
                          int nameType ,
                          long timestamp ,
                          int  vno ,
                          int  keytype ,
                          byte [] key    ){
                          
          _realm      = realm ;
          _principals = principals ;
          _timestamp  = timestamp ;
          _vno        = vno ;
          _keytype    = keytype ;
          _key        = key ;                   
      }
      public String getPrincipalName(){
         StringBuilder sb = new StringBuilder() ;
         if( _principals.length > 0 ) {
             sb.append(_principals[0]);
         }
         for( int i = 1 ; i < _principals.length ; i++ ) {
             sb.append("/").append(_principals[i]);
         }
          
         return sb.toString() ;
      }
      public int getVno(){ return _vno ; }
      public int getKeytype(){ return _keytype ; }
      public byte [] getKey(){ return _key ; }
      public String getKeytypeString(){
         switch( _keytype ){
           case 1  : return "des-cbc-crc" ;
           case 2  : return "des-cbc-md4" ;
           case 3  : return "des-cbc-md5" ;
           case 0x10 : return "des3-cbc-sha1" ;
           default : return "<Unknown("+_keytype+")>" ;
         }
      }
      public String toString(){
         return "  "+getVno()+"  "+
                     getKeytypeString()+"\t   "+
                     getPrincipalName() ;
      }
   }
   public void list(){
      Enumeration e = _list.elements() ;
      while( e.hasMoreElements() ){
         System.out.println(e.nextElement().toString());
      }
   }
   public Enumeration keyEntries(){ 
      return _list == null ? (new Vector()).elements() : _list.elements() ;
   }
   public int size(){ return _list == null ? 0 : _list.size() ; }
   public KeytabEntry getEntryAt( int i ){
      if( _list == null ) {
          throw new
                  NoSuchElementException("No Entries");
      }
      return (KeytabEntry)_list.elementAt(i) ;
   }
   public Keytab( String keytabFilename )
             throws IOException , Krb5Exception {
       loadKeytabFile( new File( keytabFilename ) ) ;         
   }
   public void loadKeytabFile( File file )
        throws IOException , Krb5Exception {
        
       DataInputStream in = 
          new DataInputStream( new FileInputStream( file ) ) ;
       
       Vector list = new Vector() ;
       try{
       
          int version = in.readByte() ;
          if( version != 5 ) {
              throw new
                      Krb5Exception(1, "Unacceptable kerberos version : " + version);
          }
          version = in.readByte() ;
          if( version != 2 ) {
              throw new
                      Krb5Exception(2, "Can only read keytab(2) not : " + version);
          }
          
          int block;
          while( true ){
             try{
                block = in.readInt() ;
             }catch(IOException ioee ){
                break ;
             }
             if( block < 0 ){
                in.skipBytes( -block ) ;
                continue ;
             }
             int nameCount = in.readShort() ;
             String realm  = in.readUTF() ;
             String [] principals = new String[nameCount] ;
             for( int i= 0 ; i < nameCount ; i++ ){
                principals[i] = in.readUTF() ;
             }
             int principalNameType = in.readInt() ;
             
             long timestamp = in.readInt() * 1000 ;
             
             int vno = in.readByte() ;
             
             int keyType = in.readShort() ;
             
             int keyLength = in.readShort() ;
             
             byte [] key = new byte[keyLength] ;
             in.readFully( key ) ;
             
             list.addElement( 
                  new KeytabEntry( 
                            realm , principals , principalNameType ,
                            timestamp , vno , keyType , key )
                             ) ;
             
          }
       }catch(EOFException eof ){
           throw new Krb5Exception(3 , "Illegal Krb5 Keytab format" ) ;
       }catch(IOException io ){
           throw io ;
       }finally{
          try{ in.close() ; }catch(Exception e){}
       }
       _list = list ;
   }
   public static void main( String [] args )throws Exception {
       if( args.length < 1 ){
          System.err.println("Usage : ... <keytabFile>");
          System.exit(4);
       }
       Keytab keytab = new Keytab( args[0] ) ;
       keytab.list() ;
       System.exit(0);
   }

}
