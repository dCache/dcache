def py_auth(public_credentials, private_credentials, principals):
    """
    Function py_auth, sample implementation for a gplazma2-pyscript plugin
    :param public_credentials: set of public auth credentials
    :param private_credentials: set of private auth credentials
    :param principals: set of principals (string values)
    :return: boolean whether authentication is permitted. The principals are
             changed as a side effect.

    Note that the set of principals may only be added to, existing principals
    may never be removed!

    Note that the principals are handled as strings in Python! Consider
    org.dcache.auth.Subjects#principalsFromArgs how these are converted back
    into principals in Java.

    In case authentication is denied, we return False.
    """
    list_accepted = [
        ("admin", "dickerelch"),
        ("Dust", "Bowl"),
        ("Rosasharn", "Joad")
    ]
    for pubcred in public_credentials:
        # public credentials iterated in random order
        # first username:password combination in list of accepted credentials leads to acceptance
        if (pubcred.getUsername(), pubcred.getPassword()) in list_accepted:
            principals.add("user:%s" % (pubcred.getUsername()))  # Python 2-style String formatting
            return True
    for privcred in private_credentials:
        # public credentials iterated in random order
        # first username:password combination in list of accepted credentials leads to acceptance
        if (privcred.getUsername(), privcred.getPassword()) in list_accepted:
            principals.add("user:%s" % (privcred.getUsername()))  # Python 2-style String formatting
            return True
    return False

def py_map(principals):
    """
    :param principals: set of principals (converted to strings)
    :return: boolean whether mapping has passed (throw AuthenticationException if False)

    In this example implementation, we just check whether some specific full-name principals
    are present.
    """
    if "user:Connie" in principals:
        # immediately fail
        return False
    elif "user:Rosasharn" in principals:
        principals.add("user:Tom")
        return True
    else:
        return True
