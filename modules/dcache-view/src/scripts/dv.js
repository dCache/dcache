(function(document) {
    'use strict';

    //console.debug(`Polymer version: ${Polymer.version}`);

    var app = document.querySelector('#app');

    // Global variables for monitoring drag and drop activities
    let dndCounter = 0;
    let timeoutID, dndArr = [];
    app.closingTime = 3000;
    app.mvObj = {};

    // Sets app default base URL
    app.baseUrl = '/';

    // See https://github.com/Polymer/polymer/issues/1381
    window.addEventListener('WebComponentsReady', function() {
        // imports are loaded and elements have been registered
        app.getQosInformation();
    });

    app.getQosInformation = function()
    {
        const isSomebody = !(app.getAuthValue() ===
            `Basic ${window.btoa('anonymous:nopassword')}`);
        if (window.CONFIG.qos === undefined && isSomebody) {
            const qos = new QosBackendInformation();
            qos.auth = app.getAuthValue();
            qos.apiEndPoint = window.CONFIG["dcache-view.endpoints.webapi"];
            qos.addEventListener('qos-backend-response', (e) => {
                window.CONFIG.qos = e.detail.response;
            });
            qos.trigger();
        }
    };

    app.menuAction = function(){
        app.$.dvDrawerPanel.togglePanel();
    };

    /**
     * List directory -> use by other elements like:
     * list-row, pagination-button, hover-contextual
     * @deprecated
     * TODO: move this to the listener
     */
    app.ls = function(path, auth)
    {
        const currentVF = findViewFile();
        let parent;
        if (currentVF) {
            parent = currentVF.parentNode;
            parent.removeChild(currentVF);
        } else {
            if (app.route === "home") {
                parent = app.$["homedir"];
            }
            if (app.route === "shared-file") {
                parent = app.$["shared-with-me"].$["shared-directory-view"].$["container"];
            }
        }

        const newVF = new ViewFile(path);
        if (auth) {
            newVF.authenticationParameters = auth;
        } else {
            if (app.route === "shared-file") {
                newVF.authenticationParameters = parent.__dataHost.authenticationParameters;
            }
        }
        parent.appendChild(newVF);
        newVF.__listDirectory();
    };

    app.namespaceView = function () {
        page("/");
        const currentVF = app.$["homedir"].querySelector('view-file');
        if (!currentVF) {
            const newVF = new ViewFile("/");
            app.$["homedir"].appendChild(newVF);
            newVF.__listDirectory();
        }
    };

    app.lsHomeDir = function()
    {
        app.route = "home";
        page("/");
        app.ls(sessionStorage.homeDirectory);
    };

    /**
     *
     * @deprecated
     * this should no longer be use for removing all
     * nodes of an element.
     * TODO: Once the implementation of mixins is complete this should be removed.
     *
     */
    app.removeAllChildren = function (node)
    {
        while (node.firstChild) {
            node.removeChild(node.firstChild);
        }
    };
    app.buildAndOpenContextMenu = function(event, contextContent, height)
    {
        app.$.centralSubContextMenu.close();
        app.$.centralContextMenu.close();
        app.removeAllChildren(app.$.centralContextMenu);
        let x = 0, y = 0;
        const w = 200;
        if (event.pageX || event.pageY) {
            x = event.pageX;
            y = event.pageY;
        } else if (event.clientX || event.clientY) {
            x = event.clientX + document.body.scrollLeft +
                document.documentElement.scrollLeft;
            y = event.clientY + document.body.scrollTop +
                document.documentElement.scrollTop;
        }
        const vx = window.innerWidth;
        const vy = window.innerHeight;
        if (vx - x < w && vy - y >= height) {
            app.x = x-w;
            app.y = y;
        } else if (vx - x < w && vy - y < height) {
            app.x = x-w;
            app.y = y-height;
        } else if (vx - x >= w && vy - y < height) {
            app.x = x;
            app.y = y-height;
        } else {
            app.x = x;
            app.y = y;
        }
        app.notifyPath('x');
        app.notifyPath('y');
        app.$.centralContextMenu.appendChild(contextContent);
        app.$.centralContextMenu.open();
    };
    app.currentDirContext = function(e)
    {
        /**
         * This code include a workaround to accommodate firefox.
         * TODO: When firefox support web components fully, revisit
         * this patch and update accordingly
         * ISSUE STATUS: https://developer.mozilla.org/en-US/docs/Web/Web_Components/Status_in_Firefox
         * See also:
         * a. https://caniuse.com/#search=Shadow%20DOM
         * b. https://caniuse.com/#search=Custom%20Elements
         */
        const vf = findViewFile(e);
        if (vf) {
            let h = 160, cc;
            if (e.screenX === 0 && e.screenY === 0) {
                const arr = e.path || (e.composedPath && e.composedPath());
                const lr = arr.find(function (el) {
                    return el.tagName === "LIST-ROW";
                });

                if (lr.xSelected && vf._xSelectedItems.length > 1) {
                    cc = new NamespaceContextualContent({files: vf._xSelectedItems}, 1, vf.authenticationParameters);
                } else {
                    cc = new NamespaceContextualContent(lr, 0, vf.authenticationParameters);
                }
                h = 310;
            } else {
                cc = new NamespaceContextualContent(vf.currentDirMetaData, 2, vf.authenticationParameters);
            }
            app.buildAndOpenContextMenu(e, cc, h)
        }
    };

    app.subContextMenu = function(e)
    {
        if (!app.$.centralSubContextMenu.opened) {
            app.removeAllChildren(app.$.centralSubContextMenu);
            const content = new ChangeQosContextualMenu(e.detail.targetNode, e.detail.authentication);

            const vx = window.innerWidth;
            const w = 198;

            app.a = vx - app.x < w * 2.1 ? app.x - w : w + app.x;
            app.b = app.y + 15;
            app.notifyPath('a');
            app.notifyPath('b');
            app.$.centralSubContextMenu.appendChild(content);
            app.$.centralSubContextMenu.open();
        }
    };

    app.click = function (e) {
        this.dispatchEvent(
            new CustomEvent('dv-namespace-reset-element-internal-parameters', {
                detail: {element: 'view-file'}, bubbles: true, composed: true}));
    };

    /**
     * Get the file name from the file path
     */
    app.getfileName = function (path)
    {
        if (path === null || path === "" || path === "/") {
            return 'Root';
        } else {
            let pt = path.endsWith('/') ? path.slice(0,-1): path;
            return pt.slice(pt.lastIndexOf('/')).substring(1);
        }
    };

    // Listing directory with time delay of @timeDelay
    app.delayedLs = function (path, timeDelay)
    {
        timeoutID = window.setTimeout(()=>{
            app.ls(path);
            dndArr = [];
            dndArr.length = 0;
        },timeDelay);
    };

    // abort request to delayed listing directory
    app.clearDelayedLs = function()
    {
        dndArr = [];
        dndArr.length = 0;
        window.clearTimeout(timeoutID);
    };

    app.delayTact = function (file)
    {
        dndArr.push(file);
        const len = dndArr.length;
        if (len === 1) {
            app.delayedLs(file.__data.filePath, 2000);
        } else if (dndArr[len - 1].__data.name !== dndArr[len - 2].__data.name) {
            app.clearDelayedLs();
        }
    };

    /**
     *
     * current view drag and drop events listeners
     */
    app.drop = function(e)
    {
        let event = e || event, iC = true, path, flag = false;
        const vf = findViewFile(e);
        if (vf) {
            if (event.detail === 0) {
                event.preventDefault && event.preventDefault();
                dndCounter = 0;
                path = vf.path;
            } else {
                const targetNode = event.detail.file;
                path = event.detail.filePath;
                iC = path === vf.path;
                targetNode.removeAttribute('in-drop-zone');
                event = event.detail.evt;
                app.clearDelayedLs();
                flag = true;
            }

            if (event.dataTransfer.types.includes('text/plain')) {
                app.dragNdropMoveFiles(path, flag);
            } else {
                app.$.dropZoneToast.close();
                const upload = new DndUpload(path, vf.authenticationParameters);
                upload.isCurrentView = iC;
                upload.start(event);
            }
        }
    };
    app.dragenter = function(e)
    {
        let event = e || event;
        let name;
        if (event.detail === 0) {
            event.preventDefault && event.preventDefault();
            dndCounter++;
            name = app.getfileName(findViewFile(event).path);
        } else {
            name = event.detail.file.fileMetaData ?
                event.detail.file.fileMetaData.fileName: event.detail.file.name;
            event = event.detail.evt;
        }

        if (!event.dataTransfer.types.includes('text/plain')) {
            app.$.dropZoneContent.querySelector('drag-enter-toast').directoryName = name;
            if (!app.$.dropZoneToast.opened){
                app.$.dropZoneToast.open();
            }
        }
    };
    app.dragleave = function()
    {
        dndCounter--;
    };
    app.dragend = function(e)
    {
        dndCounter = 0;

        //Remove all the in-drop-zone and is-dragging attributes of list-row(s)
        const vf = findViewFile(e);
        const allListRows = [...vf.$.feList.querySelectorAll('list-row')];
        const len = allListRows.length;
        for (let i=0; i<len; i++) {
            if (allListRows[i].hasAttribute('in-drop-zone')) {
                allListRows[i].removeAttribute('in-drop-zone');
            }
            if (allListRows[i].hasAttribute('is-dragging')) {
                allListRows[i].removeAttribute('is-dragging');
            }
        }
    };
    app.dragexit = function()
    {
        dndCounter = 0;
    };

    app.checkBrowser = function ()
    {
        const ua = window.navigator.userAgent;
        let tem = [];
        let M = ua.match(/(opera|chrome|safari|firefox|msie|trident(?=\/))\/?\s*(\d+)/i);

        if(/trident/i.test(M[1])) {
            tem = /\brv[ :]+(\d+.?\d*)/g.exec(ua) || [];
            return {name: 'Internet Explorer', version: tem[1]};
        } else if(/firefox/i.test(M[1])) {
            tem = /\brv[ :]+(\d+.?\d*)/g.exec(ua) || [];
            return {name: 'Firefox', version: tem[1]};
        } else if(/safari/i.test(M[1])) {
            tem = ua.match(/\bVersion\/(\d+.?\d*\s*\w+)/);
            return {name: 'Safari', version: tem[1]};
        } else if(M[1] === 'Chrome') {
            //opera
            const temOpr = ua.match(/\b(OPR)\/(\d+.?\d*.?\d*.?\d*)/);
            //edge
            const temEdge = ua.match(/\b(Edge)\/(\d+.?\d*)/);
            //chrome
            const temChrome = ua.match(/\b(Chrome)\/(\d+.?\d*.?\d*.?\d*)/);
            let genuineChrome = temOpr === null && temEdge === null && temChrome !== null;

            if(temOpr !== null) {
                return {name: temOpr[1].replace('OPR', 'Opera'), version: temOpr[2]};
            }

            if(temEdge !== null) {
                return {name: temEdge[1], version: temEdge[2]};
            }

            if(genuineChrome) {
                return {name: temChrome[1], version: temChrome[2]};
            }
        }
    };

    /**
     * Reset the duration counter for the drag and drop Toast
     */
    app.dndToastClosed = function ()
    {
        app.closingTime = 3000;
    };

    app.getAuthValue = function (authentication)
    {
        if (authentication) {
            return `${authentication.scheme} ${authentication.value}`;
        }
        if (!!sessionStorage.getItem("hasAuthClientCertificate")) {
            return "";
        }
        //kept for legacy
        if (sessionStorage.upauth !== undefined) {
            return sessionStorage.authType + ' ' + sessionStorage.upauth;
        }
        return "Basic " + window.btoa('anonymous:nopassword');
    };

    app.dragNdropMoveFiles = function (destinationPath, dropFlag)
    {
        const vf = findViewFile();
        const currentViewPath = vf.path;
        const sourcePath = ((app.mvObj.source).length > 1  && (app.mvObj.source).endsWith("/")) ?
            (app.mvObj.source).slice(0, -1) : app.mvObj.source;

        // prevent moving file if destination and source path are the same
        if (sourcePath === destinationPath) {
            return;
        }

        let auth;
        if (vf.authenticationParameters !== undefined) {
            auth = vf.authenticationParameters;
        }
        const len = app.mvObj.files.length;
        app.mvObj.files.forEach((file, i) => {
            let namespace = document.createElement('dcache-namespace');
            namespace.auth = app.getAuthValue(auth);
            namespace.promise.then( () => {
                if (currentViewPath === sourcePath) {
                    window.dispatchEvent(new CustomEvent('dv-namespace-remove-items', {
                        detail: {files: [file]},bubbles: true, composed: true}));
                } else {
                    if (!dropFlag) {
                        const item = {
                            "fileName" : file.fileName,
                            "fileMimeType" : file.fileMimeType,
                            "currentQos" : file.currentQos,
                            "size" : file.fileType === "DIR"? "--": file.size,
                            "fileType" : file.fileType,
                            "mtime" : file.mtime,
                            "creationTime" : file.creationTime
                        };
                        window.dispatchEvent(new CustomEvent('dv-namespace-add-items', {
                            detail: {files: [item]},bubbles: true, composed: true}));
                    }
                }
                return file;
            }).then((file) => {
                if (i+1 === len) {
                    openToast(`Done. ${len} files moved from source path ${sourcePath} to ${destinationPath}.`)
                }
                if (i+1 > len) {
                    openToast(`${file.fileName} moved from source path ${sourcePath} to ${destinationPath}.`);
                }
            }).catch((err)=>{
                openToast(err.toString());
            });
            namespace.mv({
                url: "/api/v1/namespace",
                path: `${sourcePath}/${file.fileName}`,
                destination: `${destinationPath}/${file.fileName}`
            });
        });
        app.mvObj = {};
    };

    function findViewFile(e)
    {
        if (app.route === "virtual") {
            return app.$["virtualDirectoriesContainer"].querySelector('view-file-labels');
        }
        else if (app.route === "home") {
            return app.$["homedir"].querySelector('view-file');
        } else if (app.route === "shared-files") {
            const fileSharingPage = app.$["shared-with-me"];
            const sharedDirectoryView = fileSharingPage.$["shared-directory-view"];
            if (!sharedDirectoryView.classList.contains("none")) {
                const len = sharedDirectoryView.$["container"].children.length;
                let j = -1;
                for (let i = 0; i < len; i++) {
                    if (sharedDirectoryView.$["container"].children[i].tagName === "VIEW-FILE") {
                        j = i;
                        break;
                    }
                }
                if (j > -1) {
                    return sharedDirectoryView.$["container"].children[j];
                }
            }
        }
    }
    function getFileWebDavUrl(path, operationType)
    {
        if (path.startsWith("/")) {
            path = path.replace(/\/\//g, "/");
        } else {
            throw new TypeError("Invalid path: only absolute path is accepted.");
        }
        const arr = window.CONFIG["dcache-view.endpoints.webdav"] !== ""
                ? [window.CONFIG["dcache-view.endpoints.webdav"]]
                : window.CONFIG["webdav"][operationType].length > 0
                        ? window.CONFIG["webdav"][operationType]
                        : [`${window.location.protocol}//${window.location.hostname}:2880`];
        const len = arr.length;
        const url = [];
        for (let i=0; i<len; i++) {
            url.push(arr[i].endsWith("/") ? `${arr[i].replace(/.$/,'')}${path}` : `${arr[i]}${path}`)
        }
        return url;
    }
    function openToast(message) {
        app.$.toast.close();
        app.$.toast.text = `${message} `;
        app.$.toast.show()
    }
    function updateFeListAndMetaDataDrawer(status, itemIndex)
    {
        if (app.$.metadata.selected === 'drawer') {
            //FIXME: use event
            app.$.metadata.querySelector('file-metadata').currentQos =
                status.constructor === Array ? `in transition to ${status[0]}` : status ;
        }
        //FIXME: use event
        const vf = findViewFile();
        vf.shadowRoot.querySelector('#feList')
            .set(`items.${itemIndex}.currentQos`, status);
        vf.shadowRoot.querySelector('#feList').notifyPath(`items.${itemIndex}.currentQos`);
    }

    window.addEventListener('qos-in-transition', function(event) {
        updateFeListAndMetaDataDrawer([`${event.detail.options.targetQos}`], event.detail.options.itemIndex);
        //make request after 0.2 seconds
        setTimeout(
            () => {
            const qosWorker = new Worker('./scripts/tasks/request-current-qos.js');
            qosWorker.addEventListener('message', function(e) {
                switch (e.data.status) {
                    case "successful":
                        updateFeListAndMetaDataDrawer(e.data.file.currentQos, event.detail.options.itemIndex);
                        openToast("Transition complete!");
                        break;
                    case "error":
                        updateFeListAndMetaDataDrawer(event.detail.options.currentQos, event.detail.options.itemIndex);
                        openToast("Transition terminated!");
                        break;
                }
                qosWorker.terminate();
            }, false);

            qosWorker.addEventListener('error', function(e) {
                console.info(e);
                openToast(e.message);
                qosWorker.terminate()
            }, false);

            qosWorker.postMessage({
                "auth": app.getAuthValue(),
                "endpoint": `${window.CONFIG["dcache-view.endpoints.webapi"]}`,
                "options": event.detail.options,
                "periodical": true
            });
        }, 200);
    });

    window.addEventListener('paper-responsive-change', function (event) {
        var narrow = event.detail.narrow;
        app.$.mainMenu.hidden = !narrow;
    });

    //Ensure that paper-input in the dialog box is always focused
    window.addEventListener('iron-overlay-opened', function(event) {
        var input = event.target.querySelector('[autofocus]');
        if (input != null) {
            switch(input.tagName.toLowerCase()) {
                case 'input':
                    input.focus();
                    break;
                case 'paper-textarea':
                case 'paper-input':
                    input.$.input.focus();
                    break;
            }
        }
    });

    // Prevent the default context menu display from right click
    window.addEventListener('contextmenu', function(event) {
        event.preventDefault();
    });

    window.addEventListener('iron-overlay-canceled', ()=> {
        const vf = findViewFile();
        vf.$.feList.selectionEnabled = false;
        setTimeout(() => {
            vf.$.feList.selectionEnabled = true;
        }, 10)
    });

    // Prevent drag and drop default behaviour on the page
    window.addEventListener('drag', function(event) {
        event.preventDefault();
        return false;
    });
    window.addEventListener('drop', function(event) {
        event.preventDefault();
        return false;
    });
    window.addEventListener('dragenter', function(event) {
        event.preventDefault();
        return false;
    });
    window.addEventListener('dragover', function(event) {
        event.preventDefault();
        return false;
    });
    window.addEventListener('admin-component-url-path', (evt)=>{
        page(evt.detail.path);
    });
    window.addEventListener('dv-namespace-dragstart', function (e) {
        app.mvObj = {};
        app.notifyPath('mvObj');

        const vf = findViewFile(e);
        app.mvObj.files= vf._xSelectedItems;
        app.mvObj.source = vf.path;

        const allListRows = [...vf.$.feList.querySelectorAll('list-row')];
        const len = vf._xSelectedItems.length;

        for (let i=0; i<len; i++) {
            const listRow = allListRows.find(function(lr) {
                return (lr.fileMetaData.fileName === vf._xSelectedItems[i].fileName);
            });
            listRow.setAttribute('is-dragging', true);
        }

        app.notifyPath('mvObj');

        e.detail.evt.dataTransfer.setData('text/plain', 'draggable');
    });
    window.addEventListener('dv-namespace-dragenter', (e)=>{
        app.dragenter(e);
        app.delayTact(e.detail.file);
    });
    window.addEventListener('dv-namespace-dragleave', (e)=>{
        app.clearDelayedLs();
    });
    window.addEventListener('dv-namespace-drop', (e)=>{
        app.drop(e);
    });
    window.addEventListener('dv-namespace-open-file', function (e) {
        let auth;
        if (e.detail.file.authenticationParameters !== undefined) {
            auth = e.detail.file.authenticationParameters;
        }
        if (e.detail.file.fileMetaData.fileType === "DIR") {
            app.ls(e.detail.file.filePath, auth);
            Polymer.dom.flush();
        } else {
            //Download the file
            const worker = new Worker('./scripts/tasks/download-task.js');
            const fileURL = getFileWebDavUrl(e.detail.file.filePath, "read")[0];
            worker.addEventListener('message', (response) => {
                worker.terminate();
                const windowUrl = window.URL || window.webkitURL;
                const url = windowUrl.createObjectURL(response.data);
                const link = app.$.download;
                link.href = url;
                link.download = e.detail.file.fileMetaData.fileName;
                link.click();
                windowUrl.revokeObjectURL(url);

            }, false);
            worker.addEventListener('error', (err)=> {
                worker.terminate();
                openToast(`${err.message}`);
            }, false);
            worker.postMessage({
                'url' : fileURL,
                'mime' : e.detail.file.fileMetaData.fileMimeType,
                'upauth' : app.getAuthValue(auth),
                'return': 'blob'
            });
        }
    });
    window.addEventListener('dv-namespace-open-subcontextmenu', e => app.subContextMenu(e));
    window.addEventListener('dv-namespace-close-subcontextmenu', () => {
        app.$.centralSubContextMenu.close();
    });

    window.addEventListener('dv-namespace-open-centralcontextmenu', () => {
        app.$.centralContextMenu.open();
    });
    window.addEventListener('dv-namespace-close-centralcontextmenu', () => {
        app.$.centralContextMenu.close();
    });
    window.addEventListener('dv-namespace-open-filemetadata-panel', e => {
        if (app.$.metadata.selected === "main") {
            let auth;
            if (e.detail.file.authenticationParameters) {
                auth = e.detail.file.authenticationParameters;
            } else if (e.detail.file.macaroon) {
                auth = {"scheme": "Bearer", "value": e.detail.file.macaroon};
            }
            app.removeAllChildren(app.$.metadataDrawer);
            const file = e.detail.file;
            const fm = file.fileMetaData ?
                new FileMetadataDashboard(Object.assign({}, file.fileMetaData), file.filePath, 0, auth) :
                new FileMetadataDashboard(
                    Object.assign({}, findViewFile().currentDirMetaData),
                    file.filePath,
                    1,
                    auth
                );
            app.$.metadataDrawer.appendChild(fm);
            app.$.metadata.openDrawer();
        } else {
            app.$.metadata.closeDrawer();
        }
    });
    window.addEventListener('dv-namespace-close-filemetadata-panel', e => {
        app.$.metadata.closeDrawer();
    });
    window.addEventListener('dv-namespace-open-central-dialogbox',(e)=>{
        app.removeAllChildren(app.$.centralDialogBox);
        app.$.centralDialogBox.appendChild(e.detail.node);
        app.$.centralDialogBox.open()
    });
    window.addEventListener('dv-namespace-close-central-dialogbox',()=>{
        app.$.centralDialogBox.close();
    });
    window.addEventListener('dv-authentication-successful', (e) => {
        window.CONFIG.isSomebody = true;
        window.CONFIG.isAdmin = e.detail.roles.includes('admin');
        app.notifyPath('config.isSomebody');
        app.notifyPath('config.isAdmin');
        app.getQosInformation();
        if (window.CONFIG['webdav'] === undefined) {
            const webdavWorker = new Worker('scripts/tasks/request-webdav-endpoints.js');

            webdavWorker.addEventListener('message', function(e) {
                window.CONFIG["webdav"] = e.data;
                webdavWorker.terminate();
            }, false);

            webdavWorker.addEventListener('error', function(e) {
                console.info(e);
                webdavWorker.terminate()
            }, false);

            webdavWorker.postMessage({
                "apiEndpoint": `${window.CONFIG["dcache-view.endpoints.webapi"]}`,
                "auth": app.getAuthValue(),
                "protocol":
                    `${window.location.protocol.endsWith(":") ?
                        window.location.protocol.slice(0, -1): window.location.protocol}`
            });
        }
        /**
         * Create Server-Side-Event Channel
         * NOTE: This required that sse-channel-utilities.js is loaded into the context
         */
        initiateSSE();
    });
    window.addEventListener('dv-namespace-open-upload-toast', (e) => {
        app.$.uploadToast.close();
        app.removeAllChildren(app.$.uploadToast.querySelector("#uploadList"));
        app.$.uploadToast.open();
    });
    window.addEventListener('dv-namespace-close-upload-toast', (e) => {
        app.$.uploadToast.close();
    });
    window.addEventListener('dv-namespace-upload-toast-append-child', (e) => {
        const child = e.detail.child;
        app.$.uploadToast.querySelector("#uploadList").appendChild(child);
    });
    window.addEventListener('dv-namespace-show-message-toast', (e) => {
        openToast(`${e.detail.message}`);
    });
    window.addEventListener('dv-namespace-ls-path', (e) => {
        if (app.route !== "home") page("/");
        app.ls(e.detail.path);
    });
    window.addEventListener('dv-namespace-open-files-viewer',(e)=>{
        app.removeAllChildren(app.$.filesViewerOverlay);
        app.$.filesViewerOverlay.appendChild(e.detail.node);
        app.$.filesViewerOverlay.open();
    });
    window.addEventListener('dv-namespace-close-files-viewer',(e)=>{
        app.$.filesViewerOverlay.close();
    });
})(document);
