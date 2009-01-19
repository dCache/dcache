package diskCacheV111.util.event ;


public class CacheEventMulticaster implements
         CacheEventListener,
         CacheRepositoryListener {
         
    protected final CacheEventListener _a , _b ;     

    protected CacheEventMulticaster(CacheEventListener a, CacheEventListener b) {
	_a = a; _b = b;
    }
    protected static CacheEventListener 
              addInternal(CacheEventListener a, CacheEventListener b) {
	if (a == null)  return b;
	if (b == null)  return a;
	return new CacheEventMulticaster(a, b);
    }
    protected static CacheEventListener 
              removeInternal(CacheEventListener l, CacheEventListener oldl) {
	if (l == oldl || l == null) {
	    return null;
	} else if (l instanceof CacheEventMulticaster) {
	    return ((CacheEventMulticaster)l).remove(oldl);
	} else {
	    return l;		// it's not here
	}
    }
    //
    //   generic cache event
    //
    public static CacheEventListener add(CacheEventListener a, CacheEventListener b) {
        return (CacheEventListener)addInternal(a, b);
    }
    public static CacheEventListener remove(CacheEventListener l, CacheEventListener oldl) {
	return (CacheEventListener) removeInternal(l, oldl);
    }
    protected CacheEventListener remove(CacheEventListener oldl) {
	if (oldl == _a)  return _b;
	if (oldl == _b)  return _a;
	CacheEventListener a2 = removeInternal(_a, oldl);
	CacheEventListener b2 = removeInternal(_b, oldl);
	if (a2 == _a && b2 == _b) {
	    return this;	// it's not here
	}
	return addInternal(a2, b2);
    }
    public void actionPerformed( CacheEvent e ){
        ((CacheEventListener)_a).actionPerformed(e);
        ((CacheEventListener)_b).actionPerformed(e);
    }
    //
    //    repository
    //
    
    public static CacheRepositoryListener add(CacheRepositoryListener a, CacheRepositoryListener b) {
        return (CacheRepositoryListener)addInternal(a, b);
    }
    public static CacheRepositoryListener remove(CacheRepositoryListener l, CacheRepositoryListener oldl) {
	return (CacheRepositoryListener) removeInternal(l, oldl);
    }
    /*
    protected CacheRepositoryListener remove(CacheRepositoryListener oldl) {
	if (oldl == _a)  return _b;
	if (oldl == _b)  return _a;
	CacheRepositoryListener a2 = removeInternal(_a, oldl);
	CacheRepositoryListener b2 = removeInternal(_b, oldl);
	if (a2 == _a && b2 == _b) {
	    return this;	// it's not here
	}
	return addInternal(a2, b2);
    }
    */
    public void actionPerformed( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).actionPerformed(e);
        ((CacheRepositoryListener)_b).actionPerformed(e);
    }
    public void precious( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).precious(e);
        ((CacheRepositoryListener)_b).precious(e);
    }
    public void cached( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).cached(e);
        ((CacheRepositoryListener)_b).cached(e);
    }
    public void sticky( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).sticky(e);
        ((CacheRepositoryListener)_b).sticky(e);
    }
    public void created( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).created(e);
        ((CacheRepositoryListener)_b).created(e);
    }
    public void touched( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).touched(e);
        ((CacheRepositoryListener)_b).touched(e);
    }
    public void removed( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).removed(e);
        ((CacheRepositoryListener)_b).removed(e);
    }
    public void destroyed( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).destroyed(e);
        ((CacheRepositoryListener)_b).destroyed(e);
    }
    public void scanned( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).scanned(e);
        ((CacheRepositoryListener)_b).scanned(e);
    }
    public void available( CacheRepositoryEvent e ){
        ((CacheRepositoryListener)_a).available(e);
        ((CacheRepositoryListener)_b).available(e);
    }
    public void needSpace( CacheNeedSpaceEvent e ){
        ((CacheRepositoryListener)_a).needSpace(e);
        ((CacheRepositoryListener)_b).needSpace(e);
    }
}
