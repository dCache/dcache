package diskCacheV111.util ;
import java.util.* ;
import java.io.PrintWriter ;
import dmg.util.*;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellSetupProvider;

/**
 * An HsmSet encapsulates information about attached HSMs. The HsmSet
 * also acts as a cell command interpreter, allowing the user to
 * add, remove or alter the information.
 *
 * Each HSM has a case sensitive instance name which uniquely
 * identifies this particular tape system throughout dCache. Notice
 * that multiple pools can be attached to the same HSM. In that case
 * the instance name must be the same at each pool.
 *
 * An HSM also has a type, e.g. OSM or Enstore. The type is not case
 * sensitive.  Traditionally, the type was called the HSM name. It is
 * important not to confuse the type with the instance name.
 *
 * Earlier versions of dCache did not specify an instance name. For
 * compatibility, the type may serve as an instance name.
 */
public class HsmSet
    implements CellCommandListener,
               CellSetupProvider
{
    private final Map<String,HsmInfo> _hsm = CollectionFactory.newHashMap();

    /**
     * Information about a particular HSM instance.
     */
    public class HsmInfo
    {
        private final String    _type;
        private final String    _instance;
        private final Map<String,String> _attr = new HashMap<>();

        /**
         * Constructs an HsmInfo object.
         *
         * @param instance A unique instance name.
         * @param type     The HSM type, e.g. OSM or enstore.
         */
        public HsmInfo(String instance, String type)
        {
            _instance = instance;
            _type = type.toLowerCase();
        }

        /**
         * Returns the instance name.
         */
        public String getInstance()
        {
            return _instance;
        }

        /**
         * Returns the HSM type (a.k.a. HSM name).
         */
        public String getType()
        {
            return _type;
        }

        /**
         * Returns the value of an attribute. Returns null if the
         * attribute has not been defined.
         *
         * @param attribute An attribute name
         */
        public String getAttribute(String attribute)
        {
           return _attr.get(attribute);
        }

        /**
         * Removes an attribute.
         *
         * @param attribute An attribute name
         */
        public void unsetAttribute(String attribute)
        {
           _attr.remove(attribute);
        }

        /**
         * Sets an attribute to a value.
         *
         * @param attribute An attribute name
         * @param value     A value string
         */
        public void setAttribute(String attribute, String value)
        {
           _attr.put(attribute, value);
        }

        /**
         * Returns the set of attributes.
         */
        public Set<Map.Entry<String, String>> attributes()
        {
            return _attr.entrySet();
        }
    }

    /**
     * Returns the set of HSMs.
     *
     * Notice that the set returned does not implement Serializable.
     */
    public Set<String> getHsmInstances()
    {
        return _hsm.keySet();
    }

    /**
     * Returns information about the named HSM. Return null if no HSM
     * with this instance name was defined.
     *
     * @param instance An HSM instance name.
     */
    public HsmInfo getHsmInfoByName(String instance)
    {
       return _hsm.get(instance);
    }


    /**
     * Returns all HSMs of a given type.
     *
     * @param type An HSM type name.
     */
    public List<HsmInfo> getHsmInfoByType(String type)
    {
        List<HsmInfo> result = new ArrayList<>(_hsm.size());
        for (HsmInfo hsm : _hsm.values()) {
            if (hsm.getType().equals(type)) {
                result.add(hsm);
            }
        }
        return result;
    }

    /**
     * Removes any information about the named HSM.
     *
     * @param instance An HSM instance name.
     */
    public void removeInfo(String instance)
    {
        _hsm.remove(instance);
    }

    /**
     * Returns the HsmInfo about the named HSM. If no such HSM is
     * known, a new HsmInfo instance is created.
     *
     * @param instance An HSM instance name.
     * @param type     An HSM type.
     */
    public HsmInfo createInfo(String instance, String type)
    {
       HsmInfo info = _hsm.get(instance);
       if (info == null) {
           _hsm.put(instance, info = new HsmInfo(instance, type));
       }
       return info;
    }

    /**
     * Scans an argument set for options and applies those as
     * attributes to an HsmInfo object.
     */
    private void _scanOptions(HsmInfo info, Args args)
    {
        for (Map.Entry<String,String> e: args.options().entries()) {
            String optName  = e.getKey();
            String optValue = e.getValue();

            info.setAttribute(optName, optValue == null ? "" : optValue);
        }
    }

    /**
     * Scans an argument set for options and removes and unsets those
     * attributes in the given HsmInfo object.
     */
    private void _scanOptionsUnset(HsmInfo info, Args args)
    {
        for (String optName: args.options().keySet()) {
            info.unsetAttribute(optName);
        }
    }

    public static final String hh_hsm_set = "<hsmType> [<hsmInstance>] [-<key>=<value>] ... ";
    public String ac_hsm_set_$_1_2(Args args)
    {
       String type = args.argv(0);
       String instance = args.argc() == 1 ? type : args.argv(1);
       HsmInfo info = createInfo(instance, type);
       _scanOptions(info, args);
       return "";
    }

    public static final String hh_hsm_unset = "<hsmInstance> [-<key>] ... ";
    public String ac_hsm_unset_$_1(Args args)
    {
       String instance = args.argv(0);
       HsmInfo info = getHsmInfoByName(instance);
       if (info == null) {
           throw new
                   IllegalArgumentException("Hsm not found : " + instance);
       }
       _scanOptionsUnset(info, args);
       return "";
    }

    public static final String hh_hsm_ls = "[<hsmInstance>] ...";
    public String ac_hsm_ls_$_0_99(Args args)
    {
       StringBuilder sb = new StringBuilder();
       if (args.argc() > 0) {
          for (int i = 0; i < args.argc(); i++) {
             _printInfos(sb, args.argv(i));
          }
       } else {
           for (String name : _hsm.keySet()) {
               _printInfos(sb, name);
           }
       }
       return sb.toString();
    }

    public static final String hh_hsm_remove = "<hsmName>";
    public String ac_hsm_remove_$_1(Args args)
    {
       removeInfo(args.argv(0));
       return "";
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        for (HsmInfo info : _hsm.values()) {
            for (Map.Entry<String,String> entry : info.attributes()) {
                pw.print("hsm set ");
                pw.print(info.getType());
                pw.print(" ");
                pw.print(info.getInstance());
                pw.print(" -");
                pw.print(entry.getKey());
                pw.print("=");
                pw.println(entry.getValue() == null ? "-" : entry.getValue());
            }
        }
    }

    @Override
    public void beforeSetup() {}

    @Override
    public void afterSetup() {}

    private void _printInfos(StringBuilder sb, String instance)
    {
        assert instance != null;

        HsmInfo info = getHsmInfoByName(instance);
        if (info == null) {
            sb.append(instance).append(" not found\n");
        } else {
            sb.append(instance).append("(").append(info.getType())
                    .append(")\n");
            for (Map.Entry<String,String> entry : info.attributes()) {
                String attrName  = entry.getKey();
                String attrValue = entry.getValue();
                sb.append("   ").
                    append(Formats.field(attrName,20,Formats.LEFT)).
                    append(attrValue == null ? "<set>" : attrValue).
                    append("\n");
            }
        }
    }
}
