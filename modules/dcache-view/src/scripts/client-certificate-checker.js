if (window.Worker && !sessionStorage.getItem("hasAuthClientCertificate")) {
    const worker = new Worker('scripts/tasks/authentication-task.js');
    worker.addEventListener('message', function (e) {
        //TODO: lock the screen
        const userData = e.data;
        worker.terminate();
        userData["upauth"] = "";
        userData["hasAuthClientCertificate"] = true;
        store(normaliseCredentialFormat(userData));
    }, false);

    worker.addEventListener('error', function (e) {
        console.warn(e);
        worker.terminate();
    }, false);
}

/**
 *
 * @param json JSON Object
 */
function store(json)
{
    for (const key in json) {
        sessionStorage.setItem(key, json[key].type === "Array" ? json[key].toString() : json[key]);
    }
}

function normaliseCredentialFormat(data, schemes)
{
    delete data["status"];
    if (schemes) {
        data["authType"] = schemes;
    }
    data["name"] = data["username"];
    delete data["username"];

    data["email"] = data.email === undefined ? "": data.email;
    data["roles"] = data.roles === undefined ? "": data.roles;

    if (data.hasOwnProperty("unassertedRoles")) {
        data["listOfPossibleRoles"] = data["unassertedRoles"];
        delete data["unassertedRoles"];
    } else {
        data["listOfPossibleRoles"] = "";
    }
    return data;
}