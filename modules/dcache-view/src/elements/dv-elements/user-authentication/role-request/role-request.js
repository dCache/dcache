class RoleRequest extends DcacheViewMixins.Commons(Polymer.Element)
{
    constructor()
    {
        super();
        this._ensureAttribute('hidden', '');
    }
    static get is()
    {
        return 'role-request';
    }
    static get properties()
    {
        return {
            promise: {
                type: Object,
                readOnly: true,
                notify: true,
                value: function() {
                    return new Promise(function (resolve, reject) {
                        this.resolveAssertion = resolve;
                        this.rejectAssertion = reject;
                    }.bind(this));
                },
            }
        };
    }
    _getB64EncodedCredential(list = "")
    {
        if (this.getAuthValue() === "Basic YW5vbnltb3VzOm5vcGFzc3dvcmQ=") {
            this.rejectAssertion("You need to authenticate to assert or un-assert roles.");
            return;
        }

        if (sessionStorage.authType !== "Basic") {
            this.rejectAssertion("Login with username and password. You cannot assert" +
                " role(s) with" +  sessionStorage.authType + "authentication scheme.");
            return;
        }

        const oldAuth = window.atob(sessionStorage.upauth);
        const newUnencodedAuth = !oldAuth.startsWith(sessionStorage.name + "#") ?
            `${oldAuth.replace(sessionStorage.name, sessionStorage.name + `#${list}`)}` :
            list === "" ? `${sessionStorage.name}${sessionStorage.password}` :
                `${sessionStorage.name}#${list}${sessionStorage.password}`;
        return window.btoa(newUnencodedAuth);
    }

    assert(listOfRolesToAssert)
    {
        try {
            if (listOfRolesToAssert === "") {
                this.rejectAssertion("You need to specify the role(s) to assert.");
                return;
            }
            const userAuth = new UserAuthentication(this._getB64EncodedCredential(listOfRolesToAssert));
            userAuth.addEventListener('dv-user-authentication-error', (e) => {
                this.rejectAssertion(e.detail.message);
            });
            userAuth.addEventListener('dv-user-authentication-successful', (e) => {
                const msg = listOfRolesToAssert.includes(',') ?
                    listOfRolesToAssert.replace(/\,(?=[^,]*$)/, "and"): listOfRolesToAssert;
                e.detail.message = `Role(s) ${msg} asserted.`;
                this.resolveAssertion(e);
            });
            userAuth.send("Basic");
        } catch(err) {
            this.rejectAssertion(err.message);
        }
    }

    /**
     *
     * @param roleToUnAssert a single a role
     */
    leave(roleToUnAssert = "*")
    {
        let auth;
        if (roleToUnAssert === "*") {
            auth = this._getB64EncodedCredential()
        } else {
            if (sessionStorage.roles && sessionStorage.roles !== "") {
                if (roleToUnAssert.includes(",")) {
                    this.rejectAssertion('Invalid role removal request! Is either you un-assert all ' +
                        'roles or a single role at a time.');
                    return;
                }
                const rolesToAssert = ((sessionStorage.roles).split(",")).map(role => {
                    if (role.trim() !== roleToUnAssert.trim()) {
                        return role.trim()
                    }
                });
                if (rolesToAssert.length > 0
                    && rolesToAssert.length <= ((sessionStorage.roles).split(",")).length) {
                    auth = this._getB64EncodedCredential(`${rolesToAssert}`);
                } else {
                    const message = `${sessionStorage.listOfPossibleRoles}`.includes(roleToUnAssert.trim()) ?
                        `asserted.`: `assigned.`;
                    this.rejectAssertion(
                        `Invalid role removal request! Role '${roleToUnAssert.trim()}' is never ${message}`
                    );
                    return;
                }
            } else {
                this.rejectAssertion('Invalid role removal request! You have not asserted any role.');
                return;
            }
        }

        const userAuth = new UserAuthentication(auth);
        userAuth.addEventListener('dv-user-authentication-error', (e) => {
            this.rejectAssertion(`Unsuccessful role removal request. ${e.detail.message}`);
        });
        userAuth.addEventListener('dv-user-authentication-successful', (e) => {
            e.detail.message = roleToUnAssert === "*" ? `All asserted roles, have been removed.`:
                `The ${roleToUnAssert.trim()} role is successfully removed.`;
            this.resolveAssertion(e);
        });
        userAuth.send("Basic");
    }
}
window.customElements.define(RoleRequest.is, RoleRequest);