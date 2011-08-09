// $Id: PartitionManager.java,v 1.5 2007-08-09 20:17:48 tigran Exp $
package diskCacheV111.poolManager ;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.dcache.cells.CellSetupProvider;
import org.dcache.cells.CellCommandListener;

import dmg.util.Args;

public class PartitionManager
    implements Serializable,
               CellCommandListener,
               CellSetupProvider
{
    static final long serialVersionUID = 3245564135066081407L;

    private final Info _defaultPartitionInfo;
    private final Map<String, Info> _infoMap =
        new HashMap<String, Info>();

    public static class Info implements Serializable
    {
        static final long serialVersionUID = -583614456111069534L;

        private final String               _name ;
        private final PoolManagerParameter _parameter  ;

        private Info( String name , Info info ){
           _name      = name ;
           _parameter = new PoolManagerParameter( info._parameter ) ;
           _parameter.setValid(false);
        }
        private Info( String name ){
           _name      = name ;
           _parameter = new PoolManagerParameter() ;
           if( name.equals("default") )_parameter.setValid(true);
        }
        public String getName(){ return _name ; }
        public PoolManagerParameter getParameter(){ return _parameter ; }
    }

    public PartitionManager(){

       _defaultPartitionInfo  = new Info("default");
       _infoMap.put( "default" , _defaultPartitionInfo ) ;

    }
    public void clear(){
       _infoMap.clear() ;
       _infoMap.put( "default" , _defaultPartitionInfo ) ;
    }
    public Map<String,PoolManagerParameter > getParameterMap(){
       Map<String,PoolManagerParameter > map = new HashMap<String,PoolManagerParameter >() ;
       for( Info info: _infoMap.values() ){
           map.put( info.getName() , new PoolManagerParameter( info.getParameter() ) ) ;
       }
       return map ;
    }
    public Info getDefaultPartitionInfo(){ return _defaultPartitionInfo ; }
    public PoolManagerParameter getParameterCopyOf( ){
       return new PoolManagerParameter( _defaultPartitionInfo.getParameter() ) ;
    }
    public PoolManagerParameter getParameterCopyOf( String partitionName ){

       PoolManagerParameter defaultParameter = new PoolManagerParameter( _defaultPartitionInfo.getParameter() ) ;
       if( partitionName == null )return defaultParameter;

       Info info = _infoMap.get( partitionName ) ;
       if( info == null )return defaultParameter ;
       return defaultParameter.merge( info.getParameter() ) ;
    }
    public Info getInfoPartitionByName( String partitionName ){

        return _infoMap.get( partitionName ) ;

    }
    /**
      *  New way of setting 'partitioned parameters'.
      *
      */
    public String hh_pmx_get_map = "" ;
    public Object ac_pmx_get_map( Args args ){
       return getParameterMap() ;
    }

    public String fh_pm_set =
       "pm set [<partitionName>|default]  OPTIONS\n"+
       "    OPTIONS\n"+
       "       -spacecostfactor=<scf>|off\n"+
       "       -cpucostfactor=<ccf>|off\n"+
       "       -max-copies=<max-replicas>|off\n"+
       "       -idle=<value>|off\n"+
       "       -p2p=<value>|off\n"+
       "       -alert=<value>|off\n"+
       "       -panic=<value>|off\n"+
       "       -fallback=<value>|off\n"+
       "       -slope=<value>|off\n"+
       "       -p2p-allowed=yes|no|off\n"+
       "       -p2p-oncost=yes|no|off\n"+
       "       -p2p-fortransfer=yes|no|off\n"+
       "       -stage-allowed=yes|no|off\n"+
       "       -stage-oncost=yes|no|off\n"+
       "";
    public String hh_pm_set = "[<partitionName>] OPTIONS #  help pm set" ;
    public String ac_pm_set_$_0_1( Args args ){
        String returnMessage = "";
        String name = args.argc() == 0 ? "default" : args.argv(0) ;
        Info info = _infoMap.get(name) ;

        //TODO: ultimately we want to remove support for this behaviour
        if( info == null ) {
          _infoMap.put( name , info = new Info( name , _defaultPartitionInfo ) ) ;
          returnMessage = "partition " + name + " created";
        }

        scanParameter( args , info._parameter ) ;

        return returnMessage;
    }

    public String hh_pm_create = "<partitionName>";
    public String ac_pm_create_$_1( Args args) {
        String name = args.argv(0);

        if( _infoMap.containsKey( name)) {
            return "Partition " + name + " already exists.";
        }

        _infoMap.put(name, new Info(name, _defaultPartitionInfo));

        return "" ;
    }
    public String hh_pm_destroy = "<partitionName> # destroys parameter partition" ;
    public String ac_pm_destroy_$_1( Args args ){

        String name = args.argv(0) ;
        if( name.equals("default") )
           throw new
           IllegalArgumentException("Can't destroy default parameter partition");

        Info info = _infoMap.get(name) ;
        if( info == null )
          throw new
          IllegalArgumentException("No such parameter partition "+name);

        _infoMap.remove( name ) ;

        return "" ;
    }
    public String hh_pm_ls = "[<section>] [-l]" ;
    public String ac_pm_ls_$_0_1( Args args ){

       StringBuffer sb = new StringBuffer() ;
       boolean extended = args.getOpt("l") != null ;
       if( args.argc() == 0 ){
          if( extended ){
              for( Info info:  _infoMap.values() ){
                  sb.append(info._name).append("\n");
                  printParameterSet( sb , info._parameter ) ;
              }
          }else{
              for( String sectionName : _infoMap.keySet() ){
                  sb.append(sectionName).append("\n");
              }
          }
       }else{
          String sectionName = args.argv(0) ;
          Info info = _infoMap.get(sectionName) ;
          if( info == null )
             throw new
             IllegalArgumentException("Section not found : "+sectionName ) ;

          sb.append(info._name).append("\n");
          printParameterSet( sb , info._parameter ) ;


       }
       return sb.toString() ;
    }
    private void printParameterSet( StringBuffer sb , PoolManagerParameter para ){

       if(para._performanceCostFactorSet)sb.append("   -cpucostfactor=").append(para._performanceCostFactor).append("\n");
       if(para._spaceCostFactorSet      )sb.append("   -spacecostfactor=").append(para._spaceCostFactor).append("\n");
       if(para._minCostCutSet           )sb.append("   -idle=").append(para._minCostCut).append("\n");
       if(para._costCutSet              )sb.append("   -p2p=").append(para.getCostCutString()).append("\n");
       if(para._alertCostCutSet         )sb.append("   -alert=").append(para._alertCostCut).append("\n");
       if(para._panicCostCutSet         )sb.append("   -halt=").append(para._panicCostCut).append("\n");
       if(para._fallbackCostCutSet      )sb.append("   -fallback=").append(para._fallbackCostCut).append("\n");
       if(para._p2pAllowedSet           )sb.append("   -p2p-allowed=").append(para._p2pAllowed).append("\n");
       if(para._p2pOnCostSet            )sb.append("   -p2p-oncost=").append(para._p2pOnCost).append("\n");
       if(para._p2pForTransferSet       )sb.append("   -p2p-fortransfer=").append(para._p2pForTransfer).append("\n");
       if(para._hasHsmBackendSet        )sb.append("   -stage-allowed=").append(para._hasHsmBackend).append("\n");
       if(para._stageOnCostSet          )sb.append("   -stage-oncost=").append(para._stageOnCost).append("\n");
       if(para._maxPnfsFileCopiesSet    )sb.append("   -max-copies=").append(para._maxPnfsFileCopies).append("\n") ;

       return ;
    }
    /**
      *  Legacy command : are treated as 'default' dCache partition.
      *
      */
    public String hh_rc_set_max_copies = "<maxNumberOfP2pCopies>" ;
    public String ac_rc_set_max_copies_$_1(Args args ){

       _defaultPartitionInfo._parameter._maxPnfsFileCopies = Integer.parseInt(args.argv(0));
       return "" ;
    }
    public String hh_set_pool_decision =
       "[-spacecostfactor=<scf>] [-cpucostfactor=<ccf>] # values for default dCache partition" ;

    public String ac_set_pool_decision( Args args )throws Exception {

       scanParameter( args , _defaultPartitionInfo._parameter ) ;

       return "scf="+_defaultPartitionInfo._parameter._spaceCostFactor+
             ";ccf="+_defaultPartitionInfo._parameter._performanceCostFactor ;
    }
    public String hh_rc_set_p2p = "on|off|oncost|fortransfer|notfortransfer" ;
    public String ac_rc_set_p2p_$_1( Args args ){

       PoolManagerParameter para = _defaultPartitionInfo._parameter ;

       String mode = args.argv(0) ;

       if( mode.equals("on") ){
          para._p2pAllowed      = true ;
          para._p2pOnCost       = false ;
          para._p2pForTransfer  = false ;
       }else if( mode.equals("oncost") ){
          para._p2pOnCost  = true ;
          para._p2pAllowed = true ;
       }else if( mode.equals("off") ){
          para._p2pOnCost  = false ;
          para._p2pAllowed = false ;
          para._p2pForTransfer  = false ;
       }else if( mode.equals("fortransfer") ){
          para._p2pForTransfer  = true ;
          para._p2pAllowed      = true ;
       }else throw new
             IllegalArgumentException("Usage : rc set p2p on|off|oncost|fortransfer");

       return "p2p="+( para._p2pAllowed ? "on" : "off" )+
              ";oncost="+( para._p2pOnCost ? "on" : "off" )+
              ";fortransfer="+( para._p2pForTransfer ? "on" : "off" ) ;
    }
    public String hh_rc_set_stage = "[oncost] on|off" ;
    public String ac_rc_set_stage_$_1_2( Args args ){

       PoolManagerParameter para = _defaultPartitionInfo._parameter ;
       String help   = "rc set stage "+hh_rc_set_stage;
       String oncost = args.argv(0) ;

       if( oncost.equals( "on" ) ){
          para._hasHsmBackend = true ;
          return "stage on" ;
       }else if( oncost.equals( "off" ) ){
          para._hasHsmBackend = false ;
          return "stage off" ;
       }else if( oncost.equals( "oncost" ) ){
          if( args.argc() < 2 )
              throw new
              IllegalArgumentException(help);

          String mode = args.argv(1) ;
          if( mode.equals("on") ){
             para._stageOnCost   = true ;
             para._hasHsmBackend = true ;
          }else if( mode.equals("off") ){
             para._stageOnCost  = false ;
          }else{
                throw new
                IllegalArgumentException(help);
          }
          return "stage-oncost="+(para._stageOnCost?"on":"off") ;
       }else
           throw new
           IllegalArgumentException(help);

    }
    public String fh_set_costcuts =
          "  set costcuts [-<options>=<value> ... ]\n"+
          "\n"+
          "   Options  |  Default  |  Description\n"+
          " -------------------------------------------------------------------\n"+
          "     idle   |   0.0     |  below 'idle' : 'reduce duplicate' mode\n"+
          "     p2p    |   0.0     |  above : start pool to pool mode\n"+
          "            |           |  If p2p value is a percent then p2p is dynamically\n"+
          "            |           |  assigned that percentile value of pool performance costs\n"+
          "     alert  |   0.0     |  stop pool 2 pool mode, start stage only mode\n"+
          "     halt   |   0.0     |  suspend system\n"+
          "   fallback |   0.0     |  Allow fallback in Permission matrix on high load\n"+
          "\n"+
          "     A value of zero disabled the corresponding value\n\n";
    public String hh_set_costcuts = "[-<option>=<value> ...] # see 'help set costcuts'" ;
    public String ac_set_costcuts( Args args ){

       PoolManagerParameter para = _defaultPartitionInfo._parameter ;

       String value = args.getOpt("idle") ;
       if( value != null )para._minCostCut = Double.parseDouble(value) ;
       value = args.getOpt("p2p") ;
       if( value != null ) para.setCostCut( value);
       value = args.getOpt("alert") ;
       if( value != null )para._alertCostCut = Double.parseDouble(value) ;
       value = args.getOpt("halt") ;
       if( value != null )para._panicCostCut = Double.parseDouble(value) ;
       value = args.getOpt("fallback") ;
       if( value != null )para._fallbackCostCut = Double.parseDouble(value) ;


       return "costcuts;idle="+para._minCostCut+
                      ";p2p="+para.getCostCutString()+
                      ";alert="+para._alertCostCut+
                      ";halt="+para._panicCostCut+
                      ";fallback="+para._fallbackCostCut ;
    }
    public String hh_rc_set_slope = "<p2p source/destination slope>" ;
    public String ac_rc_set_slope_$_1( Args args ){

       PoolManagerParameter para = _defaultPartitionInfo._parameter ;

        double d = Double.parseDouble(args.argv(0));
        if( ( d < 0.0 ) || ( d > 1.0 ) )
           throw new
           IllegalArgumentException("0 < slope < 1");

        para._slope = d ;
        return "p2p slope set to "+para._slope ;
    }
    /**
      * COSTFACTORS
      *
      *   spacecostfactor   double
      *   cpucostfactor     double
      *
      * COSTCUTS
      *
      *   idle      double
      *   p2p       double
      *   alert     double
      *   panic     double
      *   slope     double
      *   fallback  double
      *
      * P2P
      *
      *   p2p-allowed     boolean
      *   p2p-oncost      boolean
      *   p2p-fortransfer boolean
      *
      * STAGING
      *
      *   stage-allowed   boolean
      *   stage-oncost    boolean
      *
      * OTHER
      *   max-copies     int
      *
      */
    private void scanParameter( Args args , PoolManagerParameter parameter ){

       PoolManagerParameter paramDefault = _defaultPartitionInfo._parameter;

       String tmp = args.getOpt("spacecostfactor") ;
       if( tmp != null ){
          if( tmp.equals("off") ){
            parameter.unsetSpaceCostFactor();
            parameter._spaceCostFactor = paramDefault._spaceCostFactor;
          } else {
             parameter.setSpaceCostFactor(Double.parseDouble(tmp));
          }
       }
       if( ( tmp = args.getOpt("cpucostfactor") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetPerformanceCostFactor();
            parameter._performanceCostFactor = paramDefault._performanceCostFactor;
          } else {
             parameter.setPerformanceCostFactor(Double.parseDouble(tmp));
          }
       }

       if( ( tmp = args.getOpt("idle") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetMinCostCut();
            parameter._minCostCut = paramDefault._minCostCut;
          } else {
             parameter.setMinCostCut(Double.parseDouble(tmp));
          }
       }
       if( ( tmp = args.getOpt("p2p") ) != null ){
          if( tmp.equals("off") ){
            parameter.setCostCut( paramDefault.getCostCutString());
            parameter.unsetCostCut();
          } else {
             parameter.setCostCut(tmp);
          }
       }
       if( ( tmp = args.getOpt("alert") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetAlertCostCut();
                parameter._alertCostCut = paramDefault._alertCostCut;
          } else {
            parameter.setAlertCostCut(Double.parseDouble(tmp));
          }
       }
       if( ( tmp = args.getOpt("panic") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetPanicCostCut();
            parameter._panicCostCut = paramDefault._panicCostCut;
          } else {
            parameter.setPanicCostCut(Double.parseDouble(tmp));
          }
       }
       if( ( tmp = args.getOpt("slope") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetSlope();
            parameter._slope = paramDefault._slope;
          } else {
            parameter.setSlope(Double.parseDouble(tmp));
          }
       }

       if( ( tmp = args.getOpt("fallback") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetFallbackCostCut();
            parameter._fallbackCostCut = paramDefault._fallbackCostCut;
          } else {
            parameter.setFallbackCostCut(Double.parseDouble(tmp));
          }
       }

       if( ( tmp = args.getOpt("max-copies") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetMaxPnfsFileCopies();
            parameter._maxPnfsFileCopies = paramDefault._maxPnfsFileCopies;
          } else {
            parameter.setMaxPnfsFileCopies(Integer.parseInt(tmp));
          }
       }

       if( ( tmp = args.getOpt("p2p-allowed") ) != null ){
           if( tmp.equals("off") ){
               parameter.unsetP2pAllowed();
               parameter._p2pAllowed = paramDefault._p2pAllowed;

               parameter.unsetP2pOnCost();
               parameter._p2pOnCost = paramDefault._p2pOnCost;

               parameter.unsetP2pForTransfer();
               parameter._p2pForTransfer = paramDefault._p2pForTransfer;
            } else if( tmp.equals("yes") ){
               parameter.setP2pAllowed(true);
            } else if( tmp.equals("no") ) {
               parameter.setP2pAllowed(false);
               parameter.setP2pOnCost(false);
               parameter.setP2pForTransfer(false);
            } else throw new
               IllegalArgumentException("Usage: ... -p2p-allowed=yes|no|off");
        }

       if( ( tmp = args.getOpt("p2p-oncost") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetP2pOnCost();
            parameter._p2pOnCost = paramDefault._p2pOnCost;
          } else if( tmp.equals("yes")){
            parameter.setP2pOnCost(true);
            parameter.setP2pAllowed(true);
          } else if( tmp.equals("no")){
            parameter.setP2pOnCost(false);
          } else throw new
              IllegalArgumentException("Usage: ... -p2p-oncost=yes|no|off");
       }

       if( ( tmp = args.getOpt("p2p-fortransfer") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetP2pForTransfer();
            parameter._p2pForTransfer = paramDefault._p2pForTransfer;
          } else if( tmp.equals("yes") ){
            parameter.setP2pForTransfer(true);
            parameter.setP2pAllowed(true);
          } else if( tmp.equals("no") ){
            parameter.setP2pForTransfer(false);
          } else throw new
              IllegalArgumentException("Usage: ... -p2p-fortransfer=yes|no|off");
       }

       if( ( tmp = args.getOpt("stage-allowed") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetHasHsmBackend();
            parameter._hasHsmBackend = paramDefault._hasHsmBackend;

            parameter.unsetStageOnCost();
            parameter._stageOnCost = paramDefault._stageOnCost;
          } else if( tmp.equals("yes") ){
            parameter.setHasHsmBackend(true);
          } else if( tmp.equals("no") ){
            parameter.setHasHsmBackend(false);
            parameter.setStageOnCost(false);
          } else throw new
              IllegalArgumentException("Usage: ... -stage-allowed=yes|no|off");
       }

       if( ( tmp = args.getOpt("stage-oncost") ) != null ){
          if( tmp.equals("off") ){
            parameter.unsetStageOnCost();
            parameter._stageOnCost = paramDefault._stageOnCost;
          } else if( tmp.equals("yes") ){
            parameter.setStageOnCost(true);
            parameter.setHasHsmBackend(true);
          } else if( tmp.equals("no") ){
            parameter.setStageOnCost(false);
          } else throw new
              IllegalArgumentException("Usage: ... -stage-oncost=yes|no|off") ;
       }

    }

    @Override
    public void beforeSetup()
    {
        clear();
    }

    @Override
    public void afterSetup()
    {
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        dumpInfo(pw, _infoMap.get("default"));
        for (Info info: _infoMap.values()) {
            if (info.getName().equals("default"))
                continue;
            pw.append("pm create ").append(info.getName()).append("\n");
            dumpInfo(pw, info);
        }
    }

    private void dumpInfo(PrintWriter pw, Info info)
    {
        PoolManagerParameter para = info._parameter;

        pw.append("pm set ").append(info._name).append(" ");
        dumpCostOptions(pw, para);
        pw.append("\n") ;

        pw.append("pm set ").append(info._name).append(" ");
        dumpThresholdOptions(pw, para);
        pw.append("\n");

        pw.append("pm set ").append(info._name).append(" ");
        dumpP2pOptions(pw, para);
        pw.append("\n");

        pw.append("pm set ").append(info._name).append(" ");
        dumpStageOptions(pw, para);
        pw.append("\n");

        pw.append("pm set ").append(info._name).append(" ");
        dumpMiscOptions(pw, para);
        pw.append("\n");
    }

    private void dumpCostOptions(PrintWriter pw, PoolManagerParameter para)
    {
        pw.append(" -cpucostfactor=").print(para._performanceCostFactor);
        pw.append(" -spacecostfactor=").print(para._spaceCostFactor);
    }

    private void dumpThresholdOptions(PrintWriter pw, PoolManagerParameter para)
    {
        pw.append(" -idle=").print(para._minCostCut);
        pw.append(" -p2p=").print(para.getCostCutString());
        pw.append(" -alert=").print(para._alertCostCut);
        pw.append(" -halt=").print(para._panicCostCut);
        pw.append(" -fallback=").print(para._fallbackCostCut);
    }

    private void dumpP2pOptions(PrintWriter pw, PoolManagerParameter para)
    {
        if (!para._p2pAllowed) {
            pw.append(" -p2p-allowed=no");
        } else {
            pw.append(" -p2p-allowed=yes");
            pw.append(" -p2p-oncost=").print(para._p2pOnCost ? "yes":"no");
            pw.append(" -p2p-fortransfer=").print(para._p2pForTransfer ? "yes":"no");
        }
    }

    private void dumpStageOptions(PrintWriter pw, PoolManagerParameter para)
    {
        if (!para._hasHsmBackend) {
            pw.append(" -stage-allowed=no");
        } else {
            pw.append(" -stage-allowed=yes");
            pw.append(" -stage-oncost=").print(para._stageOnCost ? "yes":"no");
        }
    }

    private void dumpMiscOptions(PrintWriter pw, PoolManagerParameter para)
    {
        pw.append(" -max-copies=").print(para._maxPnfsFileCopies);
        pw.append(" -slope=").print(para._slope);
    }
}
