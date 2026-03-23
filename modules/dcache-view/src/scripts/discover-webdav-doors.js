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
    if (!!sessionStorage.getItem("hasAuthClientCertificate")) {
        return "";
    }
    if (sessionStorage.upauth !== undefined) {
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