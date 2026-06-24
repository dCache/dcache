const webdavWorker = new Worker('scripts/tasks/request-webdav-endpoints.js');

webdavWorker.addEventListener('message', function(e) {
    window.CONFIG["webdav"] = e.data;
    webdavWorker.terminate();
}, false);

webdavWorker.addEventListener('error', function(e) {
    console.info(e);
    webdavWorker.terminate()
}, false);

const getAuthValue = function (){
    console.log("CHEK the authType " + sessionStorage.authType);

    console.log("CHEK the authType " + sessionStorage.getItem("hasAuthClientCertificate"));

    if (!!sessionStorage.getItem("hasAuthClientCertificate")) {
        return "";
    }
    if (sessionStorage.upauth !== undefined) {
        console.log("sessionStorage.upauth " + `${sessionStorage.authType} ${sessionStorage.upauth}`);

        return `${sessionStorage.authType} ${sessionStorage.upauth}`;


    }
    return "";
};
webdavWorker.postMessage({
    "apiEndpoint": `${window.CONFIG["dcache-view.endpoints.webapi"]}`,
    "auth": getAuthValue(),
    "protocol":
        `${window.location.protocol.endsWith(":") ?
            window.location.protocol.slice(0, -1): window.location.protocol}`
});