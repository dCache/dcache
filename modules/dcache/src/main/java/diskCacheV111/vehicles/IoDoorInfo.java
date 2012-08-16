// $Id: IoDoorInfo.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;
import java.util.Arrays ;
import java.util.List ;

public class IoDoorInfo extends DoorInfo {

    private static final long serialVersionUID = 33390606479807121L;

   public IoDoorInfo( String cellName , String cellDomainName ){
      super( cellName , cellDomainName ) ;
   }

   public void setIoDoorEntries(IoDoorEntry [] entries)
   {
       setDetail(Arrays.copyOf(entries, entries.length));
   }

   public List<IoDoorEntry> getIoDoorEntries(){
      return Arrays.asList((IoDoorEntry [])getDetail()) ;
   }
   public String toString(){
      StringBuffer sb = new StringBuffer() ;
      sb.append(super.toString()) ;

      IoDoorEntry [] e = (IoDoorEntry [])getDetail() ;
      if( e.length > 0 )sb.append("\n");
      for( int i = 0 ; i < e.length ; i++ )
        sb.append(e[i].toString()).append("\n");
      return sb.toString();
   }
}
