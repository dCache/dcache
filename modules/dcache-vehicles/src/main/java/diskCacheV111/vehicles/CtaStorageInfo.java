package diskCacheV111.vehicles;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Implementation of the StorageInfo for CTA.
 */
public class CtaStorageInfo extends GenericStorageInfo {

    private String _family;
    private String _group;

    private static final long serialVersionUID = 1L;

    public CtaStorageInfo(String storageGroup, String fileFamily) {
        setHsm("cta");
        _family = fileFamily;
        _group = storageGroup;
        setIsNew(true);
    }


    @Override
    public String getStorageClass() {
        return (_group == null ? "None" : _group) + '.' +
              (_family == null ? "None" : _family);
    }

    public String toString() {
        return
              super.toString() +
            ";group=" + (_group == null ? "<Unknown>" : _group) +
            ";family=" + (_family == null ? "<Unknown>" : _family) + ";";
    }

    public String getStorageGroup() {
        return _group;
    }

    public String getFileFamily() {
        return _family;
    }

    @Override
    public String getKey(String key) {
        switch (key) {
            case "store":
                return _group;
            case "group":
                return _family;
            default:
                return super.getKey(key);
        }
    }

    private void readObject(java.io.ObjectInputStream stream)
          throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        if (_group != null) {
            _group = _group.intern();
        }

        if (_family != null) {
            _family = _family.intern();
        }
    }
}
