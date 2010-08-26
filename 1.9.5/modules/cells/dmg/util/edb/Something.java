package dmg.util.edb ;
import java.io.* ;
public class Something implements Serializable {
   private int _int = 44  ;
   private float _float = (float)0.55 ;
   private long  _long  = 4444444499L;
   private String _string = "Liebes schweinchen" ;
   public Something( String str , int i , float f , long l ){
     _int = i ; _float = f ; _long = l ; _string = str ;
   }
   public void writeExternal( ObjectOutput output )throws IOException {
       System.out.println( "Writing" ) ;
       output.writeInt( _int ) ;
       output.writeFloat( _float ) ;
       output.writeLong( _long ) ;
       output.writeUTF( _string ) ;
   }
   public void readExternal( ObjectInput input )throws IOException {
       System.out.println( "Reading" ) ;
       _int    = input.readInt() ;
       _float  = input.readFloat() ;
       _long   = input.readLong() ;
       _string = input.readUTF() ;
   }
   public void writeObject( ObjectOutput output )throws IOException {
       System.out.println( "Writing" ) ;
       output.writeInt( _int ) ;
       output.writeFloat( _float ) ;
       output.writeLong( _long ) ;
       output.writeUTF( _string ) ;
   }
   public void readObject( ObjectInput input )throws IOException {
       System.out.println( "Reading" ) ;
       _int    = input.readInt() ;
       _float  = input.readFloat() ;
       _long   = input.readLong() ;
       _string = input.readUTF() ;
   }
   public String toString(){
      return "int="+_int+";float="+_float+";long="+_long+";string="+_string;
   }
}
