package diskCacheV111.vehicles;

public class OSMStorageInfo extends GenericStorageInfo {

   private static final long serialVersionUID = 4260226401319935542L;

   private final String _store;
   private final String _group;

   public OSMStorageInfo( String store , String group ){
	   super();
      setHsm("osm");
      _store = store ;
      _group = group ;
      setIsNew(true) ;
   }
   public OSMStorageInfo( String store , String group , String bfid ){
	   super();
       setHsm("osm");
      _store = store ;
      _group = group ;
      setBitfileId(bfid) ;
      setIsNew(false) ;

   }
   @Override
   public String getStorageClass() {
      return (_store==null?"<Unknown>":_store) + ':' +
             (_group==null?"<Unknown>":_group) ;
   }
   public String getStore(){ return _store ; }
   public String getStorageGroup(){ return _group ; }
   @Override
   public String getKey( String key ){
       switch (key) {
       case "store":
           return _store;
       case "group":
           return _group;
       default:
           return super.getKey(key);
       }
   }
   public String toString(){
      return super.toString()+
             "store="+(_store==null?"<Unknown>":_store)+
             ";group="+(_group==null?"<Unknown>":_group)+
             ";bfid="+getBitfileId()+
             ';';

   }
}

