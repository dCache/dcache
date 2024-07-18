def py_auth(public_credentials, private_credentials, principals):
    """
    Function py_auth, sample implementation for a gplazma2-jython plugin
    :param public_credentials: set of public auth credentials
    :param private_credentials: set of private auth credentials
    :param principals: set of principals (string values)
    :return: boolean whether authentication is permitted

    Note that the set of principals may only be added to, existing principals
    may never be removed!

    Note that the principals are handled as strings in Python! Consider
    org.dcache.auth.Subjects#principalsFromArgs how these are converted back
    into principals in Java.

    In case authentication is denied, we return None.

    In this sample implementation, the following logic is used:
    AUTHENTICATE:
        Condition:
        - Either "Tom" is in the public credentials
        - Or "Rose" is in the set of private credentials
        Result:
        - the username principal "Joad" is added (as string "username:joad")
        - we return True
    REFUSE:
        Condition:
        - Either "Connie" is in either one of the credentials
        - No passing condition from above is met
        Result:
        - we return False
    """
    if ("Connie" in public_credentials) or ("Connie" in private_credentials):
        return False
    elif ("Tom" in public_credentials) or ("Rose" in private_credentials):
        principals.add("username:Joad")
        return True
    else:
        return False