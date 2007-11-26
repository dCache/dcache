package diskCacheV111.util.event ;


    public class XXX implements CacheRepositoryListener {
        private String _name = null ;
        public XXX( String name ){ _name = name ; }
        public void actionPerformed( CacheEvent event ){
           System.out.println("Event received by "+_name ) ;
           Object source = event.getSource() ;
           System.out.println(_name+" : "+source.getClass().getName() ) ;
           if( source instanceof CacheComponent ){
              if( _name.equals("x2") ){
                ((CacheComponent)source).removeCacheRepositoryListener( this ) ;
                System.out.println("Removing "+_name ) ;
              }
           }
        }
        public void precious( CacheRepositoryEvent event ){}
        public void available( CacheRepositoryEvent event ){}
        public void cached( CacheRepositoryEvent event ){}
        public void sticky( CacheRepositoryEvent event ){}
        public void created( CacheRepositoryEvent event ){
           System.out.println("Created");
        }
        public void touched( CacheRepositoryEvent event ){
           System.out.println("Added");
        }
        public void removed( CacheRepositoryEvent event ){}
        public void destroyed( CacheRepositoryEvent event ){}
        public void scanned( CacheRepositoryEvent event ){}
        public void needSpace( CacheNeedSpaceEvent event ) {}
    }
