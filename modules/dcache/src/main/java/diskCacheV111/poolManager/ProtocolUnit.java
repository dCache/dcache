package diskCacheV111.poolManager;

class ProtocolUnit extends Unit {
    private static final long serialVersionUID = 4588437437939085320L;
    private final String _protocol;
    private int _version = -1;

    ProtocolUnit(String name) {
        super(name, PoolSelectionUnitV2.PROTOCOL);

        int pos = name.indexOf('/');
        if ((pos < 0) || (pos == 0) || ((name.length() - 1) == pos)) {
            throw new IllegalArgumentException("Wrong format for protocol unit <protocol>/<version>");
        }
        _protocol = name.substring(0, pos);
        String version = name.substring(pos + 1);
        try {
            _version = version.equals("*") ? -1 : Integer.parseInt(version);
        } catch (Exception ee) {
            throw new IllegalArgumentException("Wrong format : Protocol version must be * or numerical");
        }
    }

    @Override
    public String getName() {
        return _protocol + (_version > -1 ? ("/" + _version) : "/*");
    }

}
