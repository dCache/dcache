package diskCacheV111.poolManager ;

import java.util.HashMap;
import java.util.Map;

public class  PoolManagerParameter implements java.io.Serializable {

    static final long serialVersionUID = -3402641110205054015L;

    static final int P2P_SAME_HOST_BEST_EFFORT = 0 ;
    static final int P2P_SAME_HOST_NEVER       = 1 ;
    static final int P2P_SAME_HOST_NOT_CHECKED = 2 ;

    //
    // The parameters
    //
    int     _allowSameHostCopy = P2P_SAME_HOST_BEST_EFFORT ;
    int     _maxPnfsFileCopies = 500 ;

    boolean _p2pAllowed        = true ;
    boolean _p2pOnCost         = false ;
    boolean _p2pForTransfer    = false ;

    boolean _hasHsmBackend     = false ;
    boolean _stageOnCost       = false ;

    double  _slope             = 0.0 ;
    double  _minCostCut        = 0.0 ;
    private double  _costCut           = 0.0 ;
    private boolean _costCutIsPercentile = false ;
    double  _alertCostCut      = 0.0 ;
    double  _panicCostCut      = 0.0 ;
    double  _fallbackCostCut   = 0.0 ;

    double  _spaceCostFactor       = 1.0 ;
    double  _performanceCostFactor = 1.0 ;

    //
    // is parameter set ?
    //
    boolean _allowSameHostCopySet = false ;
    boolean _maxPnfsFileCopiesSet = false ;

    boolean _p2pAllowedSet      = false ;
    boolean _p2pForTransferSet  = false ;
    boolean _p2pOnCostSet       = false ;

    boolean _stageOnCostSet    = false ;
    boolean _hasHsmBackendSet  = false ;

    boolean _slopeSet           = false ;
    boolean _minCostCutSet      = false ;
    boolean _costCutSet         = false ;
    boolean _alertCostCutSet    = false ;
    boolean _panicCostCutSet    = false ;
    boolean _fallbackCostCutSet = false ;

    boolean _performanceCostFactorSet  = false ;
    boolean _spaceCostFactorSet        = false ;

    PoolManagerParameter(){}
    PoolManagerParameter( PoolManagerParameter copy ){

       _allowSameHostCopy = copy._allowSameHostCopy ; _allowSameHostCopySet = copy._allowSameHostCopySet ;
       _maxPnfsFileCopies = copy._maxPnfsFileCopies ; _maxPnfsFileCopiesSet = copy._maxPnfsFileCopiesSet ;

       _p2pAllowed        = copy._p2pAllowed ; _p2pAllowedSet         = copy._p2pAllowedSet ;
       _p2pOnCost         = copy._p2pOnCost  ; _p2pOnCostSet          = copy._p2pOnCostSet ;
       _p2pForTransfer    = copy._p2pForTransfer ; _p2pForTransferSet = copy._p2pForTransferSet ;

       _hasHsmBackend     = copy._hasHsmBackend ; _hasHsmBackendSet     = copy._hasHsmBackendSet ;
       _stageOnCost       = copy._stageOnCost ;   _stageOnCostSet       = copy._stageOnCostSet ;

       _slope             = copy._slope ;           _slopeSet           = copy._slopeSet ;
       _minCostCut        = copy._minCostCut ;      _minCostCutSet      = copy._minCostCutSet ;
       _costCut           = copy._costCut;          _costCutSet         = copy._costCutSet;
       _costCutIsPercentile = copy._costCutIsPercentile;
       _alertCostCut      = copy._alertCostCut;     _alertCostCutSet    = copy._alertCostCutSet;
       _panicCostCut      = copy._panicCostCut ;    _panicCostCutSet    = copy._panicCostCutSet ;
       _fallbackCostCut   = copy._fallbackCostCut ; _fallbackCostCutSet = copy._fallbackCostCutSet ;

       _spaceCostFactor       = copy._spaceCostFactor ;       _spaceCostFactorSet       = copy._spaceCostFactorSet ;
       _performanceCostFactor = copy._performanceCostFactor ; _performanceCostFactorSet = copy._performanceCostFactorSet ;

    }
    PoolManagerParameter setValid( boolean valid ){
       _allowSameHostCopySet = valid ;
       _maxPnfsFileCopiesSet = valid ;

       _p2pAllowedSet        = valid ;
       _p2pOnCostSet         = valid ;
       _p2pForTransferSet    = valid ;

       _hasHsmBackendSet     = valid ;
       _stageOnCostSet       = valid ;

       _slopeSet             = valid ;
       _minCostCutSet        = valid ;
       _costCutSet           = valid ;
       _alertCostCutSet      = valid ;
       _panicCostCutSet      = valid ;
       _fallbackCostCutSet   = valid ;

       _spaceCostFactorSet       = valid ;
       _performanceCostFactorSet = valid ;
       return this ;
    }
    Map<String, Object[]> toMap(){

       Map<String, Object[]> map  = new HashMap<String, Object[]>() ;
       map.put( "p2p-allowed"     , new Object[]{ Boolean.valueOf(_p2pAllowedSet)     , Boolean.valueOf(_p2pAllowed) } ) ;
       map.put( "p2p-oncost"      , new Object[]{ Boolean.valueOf(_p2pOnCostSet)      , Boolean.valueOf(_p2pOnCost) } ) ;
       map.put( "p2p-fortransfer" , new Object[]{ Boolean.valueOf(_p2pForTransferSet) , Boolean.valueOf(_p2pForTransfer) } ) ;
       map.put( "stage-allowed"   , new Object[]{ Boolean.valueOf(_hasHsmBackendSet)  , Boolean.valueOf(_hasHsmBackend) } ) ;
       map.put( "stage-oncost"    , new Object[]{ Boolean.valueOf(_stageOnCostSet)    , Boolean.valueOf(_stageOnCost) } ) ;

       map.put( "slope"    , new Object[]{ Boolean.valueOf(_slopeSet)           , new Double(_slope) } ) ;
       map.put( "idle"     , new Object[]{ Boolean.valueOf(_minCostCutSet)      , new Double(_minCostCut) } ) ;
       map.put( "p2p"      , new Object[]{ Boolean.valueOf(_costCutSet)         , new Double(_costCut) } ) ;
       map.put( "alert"    , new Object[]{ Boolean.valueOf(_alertCostCutSet)    , new Double(_alertCostCut) } ) ;
       map.put( "panic"    , new Object[]{ Boolean.valueOf(_panicCostCutSet)    , new Double(_panicCostCut) } ) ;
       map.put( "fallback" , new Object[]{ Boolean.valueOf(_fallbackCostCutSet) , new Double(_fallbackCostCut) } ) ;

       map.put( "spacecostfactor" , new Object[]{ Boolean.valueOf(_spaceCostFactorSet)       , new Double(_spaceCostFactor) } ) ;
       map.put( "cpucostfactor"   , new Object[]{ Boolean.valueOf(_performanceCostFactorSet) , new Double(_performanceCostFactor) } ) ;

       map.put( "max-pnfs-copies"    , new Object[]{ Boolean.valueOf(_maxPnfsFileCopiesSet) , Integer.valueOf(_maxPnfsFileCopies) } ) ;
       map.put( "same-host-copies"   , new Object[]{ Boolean.valueOf(_allowSameHostCopySet) , Integer.valueOf(_allowSameHostCopy) } ) ;

      return map ;
    }
    PoolManagerParameter merge( PoolManagerParameter copy ){

       if( _p2pOnCostSet      = copy._p2pOnCostSet      )_p2pOnCost         = copy._p2pOnCost ;
       if( _p2pForTransferSet = copy._p2pForTransferSet )_p2pForTransfer    = copy._p2pForTransfer ;
       if( _stageOnCostSet    = copy._stageOnCostSet    )_stageOnCost       = copy._stageOnCost ;
       if( _p2pAllowedSet     = copy._p2pAllowedSet     )_p2pAllowed        = copy._p2pAllowed ;
       if( _slopeSet          = copy._slopeSet          )_slope             = copy._slope ;
       if( _costCutSet        = copy._costCutSet        ) {
           _costCut           = copy._costCut;
           _costCutIsPercentile = copy._costCutIsPercentile;
       }
       if( _alertCostCutSet   = copy._alertCostCutSet   )_alertCostCut      = copy._alertCostCut;
       if( _panicCostCutSet   = copy._panicCostCutSet   )_panicCostCut      = copy._panicCostCut ;
       if( _minCostCutSet     = copy._minCostCutSet     )_minCostCut        = copy._minCostCut ;
       if( _maxPnfsFileCopiesSet = copy._maxPnfsFileCopiesSet )_maxPnfsFileCopies = copy._maxPnfsFileCopies ;
       if( _allowSameHostCopySet = copy._allowSameHostCopySet )_allowSameHostCopy = copy._allowSameHostCopy ;
       if( _fallbackCostCutSet   = copy._fallbackCostCutSet   )_fallbackCostCut   = copy._fallbackCostCut ;
       if( _hasHsmBackendSet     = copy._hasHsmBackendSet     )_hasHsmBackend     = copy._hasHsmBackend ;
       if( _spaceCostFactorSet   = copy._spaceCostFactorSet   )_spaceCostFactor   = copy._spaceCostFactor ;
       if( _performanceCostFactorSet  = copy._performanceCostFactorSet  )_performanceCostFactor = copy._performanceCostFactor ;

       return this ;
    }
    void setAllowSameHostCopy( int value ){ _allowSameHostCopy = value ; _allowSameHostCopySet = true ; }
    void unsetAllowSameHostCopy(){ _allowSameHostCopySet = false ; }
    void setP2pOnCost( boolean value ){ _p2pOnCost = value ;  _p2pOnCostSet = true ; }
    void unsetP2pOnCost(){ _p2pOnCostSet = false ; }
    void setP2pForTransfer( boolean value ){ _p2pForTransfer = value ;  _p2pForTransferSet = true ; }
    void unsetP2pForTransfer(){ _p2pForTransferSet = false ; }
    void setHasHsmBackend( boolean value ){ _hasHsmBackend = value ;  _hasHsmBackendSet = true ; }
    void unsetHasHsmBackend(){ _hasHsmBackend = false ; }
    void setStageOnCost( boolean value ){ _stageOnCost = value ;  _stageOnCostSet = true ; }
    void unsetStageOnCost(){ _stageOnCostSet = false ; }
    void setP2pAllowed( boolean value ){ _p2pAllowed = value ;  _p2pAllowedSet = true ; }
    void unsetP2pAllowed(){ _p2pAllowedSet = false ; }
    void setSlope( double value ){ _slope = value ;  _slopeSet = true ;  }
    void unsetSlope(){ _slopeSet = false ; }

    /**
     * Obtain the threshold cost for triggering p2p transfers on cost:
     * transfers involving reading from a pool with cost greater than the
     * costCut may trigger a pool-to-pool transfer.  The value returned is
     * either an absolute measure of cost or a percentile value.  Whether the
     * number is an absolute cost or a percentile cost is discoverable by
     * calling the {@link #isCostCutPercentile()} method.
     * <p>
     * If the value is absolute then the returned value may be used directly
     * when evaluating whether the cost for a transfer exceeds the threshold
     * for triggering a pool-to-pool transfer.
     * <p>
     * If the number is a percentile value then the corresponding threshold is
     * determined dynamically.  If f is the value returned by
     * <code>getCostCut()</code> and a list of N pools is sorted in ascending
     * performance cost then the value used is the cost of pool with index
     * floor(fN).
     * <p>
     * @return the threshold cost for pool-to-pool transfers
     */
    double getCostCut() {
        return _costCut;
    }

    /**
     * Assign a new value for the threshold at which read requests will trigger
     * pool-to-pool transfers.  The value may be optionally suffixed with a
     * percent symbol; doing so will result in the value/100 being stored and
     * subsequent calls to {@link #isCostCutPercentile()}
     * returning true.
     * @param value the String representation of the new costCut value.
     * @see #getCostCut()
     */
    void setCostCut( String value ) {
        if( value.endsWith( "%")) {
            String numberPart = value.substring( 0, value.length()-1);
            _costCut = Double.parseDouble( numberPart)/100;
            setCostCutIsPercentile( true);
        } else {
            _costCut = Double.parseDouble(value) ;
            setCostCutIsPercentile( false);
        }
        _costCutSet = true ;
    }

    /**
     * Mark that the current costCut value should be interpreted as having a
     * relative value compared to the maximum CPU cost of all pools.
     * @param isPercentile true if the costCut should be interpreted as relative,
     * false otherwise.
     * @throws IllegalArgumentException if the transition would result in an illegal state.
     */
    void setCostCutIsPercentile( boolean isPercentile) {
        if( isPercentile) {
            if( _costCut <= 0)
                throw new IllegalArgumentException( "Number " + isPercentile + "is too small; must be > 0%");
            if( _costCut >= 1)
                throw new IllegalArgumentException( "Number " + isPercentile + " is too large; must be < 100%");
        }
        _costCutIsPercentile = isPercentile;
    }

    /**
     * Discover whether the current costCut value should be interpreted as
     * the percentile value.
     */
    boolean isCostCutPercentile() {
        return _costCutIsPercentile;
    }

    /**
     * Mark that the costCut value is no longer valid.
     */
    void unsetCostCut(){
        _costCutSet = false;
    }

    /**
     * Obtain a String representation of the current costCut value.
     * <p>
     * This method returns a String that, for any costCut value, can be
     * supplied to {@link #setCostCut(String)} without changing its value.
     * <p>
     * If the current costCut uses a percentile value then the String
     * representation will be the value, as a percentage, appended with the
     * percent symbol otherwise the current value is returned.
     * @return a String representation of the current cost cut
     * @see #setCostCut(String)
     */
    String getCostCutString() {
        if( _costCutIsPercentile) {
            return Double.toString(  _costCut * 100) + "%";
        } else {
            return Double.toString(  _costCut);
        }
    }
    void setAlertCostCut( double value ){ _alertCostCut = value ; _alertCostCutSet = true ; }
    void unsetAlertCostCut(){ _alertCostCutSet = false ; }
    void setPanicCostCut( double value ){ _panicCostCut = value ; _panicCostCutSet = true ; }
    void unsetPanicCostCut(){ _panicCostCutSet = false ; }
    void setMinCostCut( double value ){ _minCostCut = value ; _minCostCutSet = true ; }
    void unsetMinCostCut(){ _minCostCutSet = false ; }
    void setFallbackCostCut( double value ){ _fallbackCostCut = value ; _fallbackCostCutSet = true ; }
    void unsetFallbackCostCut(){ _fallbackCostCutSet = false ; }
    void setMaxPnfsFileCopies( int value ){ _maxPnfsFileCopies = value ; _maxPnfsFileCopiesSet = true ; }
    void unsetMaxPnfsFileCopies(){ _maxPnfsFileCopiesSet = false ; }
    void setPerformanceCostFactor( double value ){ _performanceCostFactor = value ; _performanceCostFactorSet = true ; }
    void unsetPerformanceCostFactor(){ _performanceCostFactorSet = false ; }
    void setSpaceCostFactor( double value ){ _spaceCostFactor = value ; _spaceCostFactorSet = true ; }
    void unsetSpaceCostFactor(){ _spaceCostFactorSet = false ; }
}
