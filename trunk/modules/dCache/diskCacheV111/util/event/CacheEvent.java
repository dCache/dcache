package diskCacheV111.util.event ;

public class CacheEvent {
   private Object _source = null ;
   public CacheEvent(){}
   public CacheEvent( Object source ){
       _source = source ;
   }
   public Object getSource(){ return _source ; }

}
