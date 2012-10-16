package dmg.security.digest ;

public class DigestTest {

  static public String byteToHexString( byte b ) {
       String s = Integer.toHexString( ( b < 0 ) ? ( 256 + (int)b ) : (int)b  ) ;
       if( s.length() == 1 ) {
           return "0" + s;
       } else {
           return s;
       }
  }
  static public String byteToHexString( byte [] bytes ) {
      
	  StringBuilder sb = new StringBuilder(bytes.length +1);

      for (byte aByte : bytes) {
          sb.append(byteToHexString(aByte)).append(" ");
      }
       return sb.toString() ;    
  }
  private static String _use = "DigestTest crc32|zipcrc32|md5  <data>" ;
  
  public static void main( String [] args ){
     if( args.length < 1 ){
        System.out.println( " Usage : "+_use ) ;
        System.exit(2) ;
     }
     String  type = args[0] ;
     byte [] data = args[1].getBytes() ;
     MsgDigest digest = null ;
      switch (type) {
      case "crc32":
          digest = new Crc32();
          break;
      case "zipcrc32":
          digest = new ZipCrc32();
          break;
      case "md5":
          try {
              digest = new Md5();
          } catch (Exception e) {
              System.out.println(" Exception  : " + e);
          }
          break;
      default:
          try {
              digest = new GenDigest(type);
          } catch (Exception e) {
              System.out.println(" Exception  : " + e);
              System.out.println(" Usage : " + _use);
              System.exit(2);
          }
          break;
      }
     
     digest.update( data  ) ;
     
     byte [] res = digest.digest() ;
     
     System.out.println( " Data   : "+args[1] ) ;
     System.out.println( " Type   : "+type ) ;
     System.out.println( " Result : "+byteToHexString( res ) ) ;
     
  
  }


}
