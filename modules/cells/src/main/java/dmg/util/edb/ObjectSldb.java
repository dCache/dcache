package dmg.util.edb ;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ObjectSldb extends Sldb {

   public ObjectSldb( File file )throws IOException {
      super(file);
   }
   public ObjectSldb( File file , int bpdr , int rpb )throws IOException {
      super(file,bpdr,rpb);
   }
   public SldbEntry writeObject( Object obj )throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
        ObjectOutputStream oos = new ObjectOutputStream(baos) ;
        oos.writeObject( obj ) ;
        oos.close() ;
        byte [] data = baos.toByteArray() ;
        System.out.println( "Writing "+data.length+" bytes" ) ;
        return put( data , 0 , data.length ) ;
   }
   public SldbEntry writeObject( SldbEntry e , Object obj )throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
        ObjectOutputStream oos = new ObjectOutputStream(baos) ;
        oos.writeObject( obj ) ;
        oos.close() ;
        byte [] data = baos.toByteArray() ;
        return put( e , data , 0 , data.length ) ;
   }
   public Object readObject( SldbEntry e )
                  throws IOException,ClassNotFoundException  {
      byte   [] data = get( e ) ;
      System.out.println( "Got "+data.length+" byte" ) ;
      ByteArrayInputStream bais = new ByteArrayInputStream(data) ;
      ObjectInputStream ois = new ObjectInputStream(bais) ;

      return ois.readObject() ;
   }
   public static void main( String [] args ) throws Exception {
      ObjectSldb sldb = new ObjectSldb( new File("xxx")) ;
//      Something s = new Something( "hallo trude",4,(float)5,5L);
//      sldb.writeObject( s ) ;
       Object obj = sldb.readObject(sldb.getEntry(27));
       System.out.println( obj.toString() ) ;
      sldb.close() ;
   }
}
