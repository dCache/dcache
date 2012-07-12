package dmg.util.edb ;

import java.io.* ;

public class Jdbm {
   public class InputStreamHelper extends InputStream {
       private RandomAccessFile _in = null ;
       public InputStreamHelper( RandomAccessFile in ){
          _in = in ;
       }
       @Override
       public int read() throws IOException {
          return _in.read() ;
       }
   }
   public class OutputStreamHelper extends OutputStream {
       private DataOutput _out = null ;
       public OutputStreamHelper( DataOutput out ){
          _out = out ;
       }
       @Override
       public void write( int i ) throws IOException {
           _out.write(i) ;
       }
   }
   public Jdbm( String [] args ) throws Exception {
      if( args.length < 2 ){
         System.err.println( "Usage : ... <filename> <command>" ) ;
         System.err.println( "Commands :  read" ) ;
         System.exit(0);
      }
       RandomAccessFile f = new RandomAccessFile( new File(args[0])  , "rw" ) ;
       
       JdbmObjectOutputStream out =
         new JdbmObjectOutputStream(
              new DataOutputStream(
                   new OutputStreamHelper( f ) ) ) ;
       JdbmObjectInputStream in =
         new JdbmObjectInputStream(
              new DataInputStream(
                   new InputStreamHelper( f ) ) ) ;
       
       if( args[1].equals("read" ) ){
          JdbmFileHeader header = (JdbmFileHeader)in.readObject()  ;
          System.out.println( header.toString() ) ;
          f.seek( header.getDirectoryAddress() ) ;
          int dirSize = header.getDirectorySize() ;
          long [] x = new long[dirSize] ;
          in.readLongArray( x ) ;

          f.close() ;   
       }else{
          JdbmFileHeader header = new JdbmFileHeader(1024) ;
          System.out.println( header.toString() ) ;
          header.expandDirectory() ;
          System.out.println( header.toString() ) ;
          out.writeObject( header ) ;
          out.flush() ;
          f.seek( 1024L ) ;
          out.writeObject( header.getDirectory() ) ;

          f.close() ;   
       }
   
   }
   public static void main(String [] args )throws Exception {
        new Jdbm( args ) ;
   }
}
