class SharedFileList extends DcacheViewMixins.Commons(Polymer.Element)
{
    static get is()
    {
        return 'shared-file-list';
    }
    constructor()
    {
        super();
        this._addSharedFileListener = this._addSharedFile.bind(this);
        this.macaroonList = !!sessionStorage.macaroonList ? sessionStorage.macaroonList.split(" ") : [];
        this._reset_ = this._clearSelection.bind(this);
        this._removeItemsListener = this._removeItems.bind(this);
        this._renameListener = this._renameInputStart.bind(this);
    }
    connectedCallback()
    {
        super.connectedCallback();
        this.addEventListener('dv-namespace-rename-input', this._renameInputEnd);
        window.addEventListener('dv-namespace-file-sharing-add-macaroon', this._addSharedFileListener);
        window.addEventListener('dv-namespace-reset-element-internal-parameters', this._reset_);
        window.addEventListener('dv-namespace-file-sharing-remove-items', this._removeItemsListener);
        window.addEventListener('dv-file-sharing-open-rename-dialogbox', this._renameListener);
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        this.removeEventListener('dv-namespace-rename-input', this._renameInputEnd);
        window.removeEventListener('dv-namespace-file-sharing-add-macaroon', this._addSharedFileListener);
        window.removeEventListener('dv-namespace-reset-element-internal-parameters', this._reset_);
        window.removeEventListener('dv-namespace-file-sharing-remove-items', this._removeItemsListener);
        window.removeEventListener('dv-file-sharing-open-rename-dialogbox', this._renameListener);
    }
    static get properties()
    {
        return {
            macaroonList: {
                type: Array,
                notify: true,
                observer: '_macaroonListChanged'
            },
            sharedFiles: {
                type: Array,
                value: [],
                notify: true
            },
            sort: {
                type: Object,
                value: {isSorted: false, columnKey : null},
                notify: true
            },
            selectionMonitoring: {
                type: Array,
                value: [],
                notify: true,
            },
            multipleSelection: {
                type: Boolean,
                value: false,
                notify: true,
            }
        }
    }
    static get observers() {
        return [
            '_update(sharedFiles.length)'
        ]
    }
    _macaroonListChanged(macaroons)
    {
        if (macaroons.length > 0) {
            this._deserialiseMacaroons(macaroons).then((response) => {
                response.forEach((child) => {
                    this.push('sharedFiles', child);
                    this._appendFM(child);
                });
            }).catch((err) => {
                this.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast', {
                    detail: {message: err.message}, bubbles: true, composed: true}));
            })
        }
    }
    _deserialiseMacaroons(macaroonsArray)
    {
        return new Promise((resolve, reject) => {
            if (macaroonsArray.length > 0) {
                const macaroonDeserialiserWorker = new Worker('./scripts/tasks/macaroon-deserialise-task.js');

                macaroonDeserialiserWorker.addEventListener('message', (e) => {
                    resolve(e.data);
                    macaroonDeserialiserWorker.terminate();
                }, false);
                macaroonDeserialiserWorker.addEventListener('error', (e)=> {
                    macaroonDeserialiserWorker.terminate();
                    reject(e);
                }, false);
                macaroonDeserialiserWorker.postMessage({
                    'macaroons' : macaroonsArray
                });
            } else {
                reject("The argument must be array and must not be empty.");
            }
        });
    }
    _openAddMacaroonForm()
    {
        const addMacaroonDialog = new AddMacaroonForm();
        this.dispatchEvent(
            new CustomEvent('dv-namespace-open-central-dialogbox', {
                detail: {node: addMacaroonDialog}, bubbles: true, composed: true}));
    }
    _addSharedFile(e)
    {
        if (!!sessionStorage.macaroonList && sessionStorage.macaroonList.includes(e.detail.macaroon)) {
            if (e.detail.macaroon === "") {
                this.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast', {
                    detail: {message: "Please paste the macaroon you want to add to your list."},
                    bubbles: true, composed: true}));
            } else {
                this.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast', {
                    detail: {message: "This macaroon is already added on the shared file list."},
                    bubbles: true, composed: true}));
            }

            this.dispatchEvent(new CustomEvent('dv-namespace-file-sharing-add-macaroon-error', {
                bubbles: true, composed: true
            }));
            return
        }
        this._deserialiseMacaroons([e.detail.macaroon]).then((response) => {
            response.forEach((child) => {
                if (this.sort.isSorted) {
                    const index = this.sharedFiles.findIndex((file) => {
                        return this._getSubPropertyValue(file, this.sort.columnKey, this.sort.type) >
                            this._getSubPropertyValue(child, this.sort.columnKey, this.sort.type)
                    });
                    this.splice('sharedFiles', index === -1 ? this.sharedFiles.length: index, 0, child);
                } else {
                    this.push('sharedFiles', child);
                }
                this._appendFM(child);
            });

            this.dispatchEvent(new CustomEvent('dv-namespace-close-central-dialogbox', {
                bubbles: true, composed: true
            }));
            sessionStorage.macaroonList = !!sessionStorage.macaroonList ?
                `${sessionStorage.macaroonList} ${e.detail.macaroon}` : e.detail.macaroon;
        }).catch((err) => {
            this.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast', {
                detail: {message: err.message}, bubbles: true, composed: true}));
            this.dispatchEvent(new CustomEvent('dv-namespace-file-sharing-add-macaroon-error', {
                bubbles: true, composed: true
            }));
        });
    }
    _update(ls)
    {
        if (ls > 0 && this.$['list'].classList.contains('none')) {
            this.$['list'].classList.replace('none', 'list');
            this.$['empty'].classList.replace('empty', 'none');
        } else if (!sessionStorage.macaroonList) {
            this.$['list'].classList.replace('list', 'none');
            this.$['empty'].classList.replace('none', 'empty');
        }
    }
    _isExpired(date)
    {
        const present =  new Date().getTime();
        const expirationDate = new Date(date).getTime();
        return present - expirationDate > 0;
    }
    _convertTime(time)
    {
        return new Date(time);
    }
    _checkOwner(owner)
    {
        return owner === sessionStorage.name ? 'me' : owner;
    }
    _styleActivities(activities)
    {
        return activities === "" || activities === undefined ?
            "all available activities are allowed" : activities.replace(/,/g, ", ");
    }
    _compare(columnKey, dataType)
    {
        this.sharedFiles.sort((a, b) => {
            const A = this._getSubPropertyValue(a, columnKey, dataType);
            const B = this._getSubPropertyValue(b, columnKey, dataType);

            if (A < B) {
                return -1;
            } else if (A > B) {
                return 1;
            } else {
                return 0;
            }
        });
    }
    _getSubPropertyValue(object, keys, dataType)
    {
        const properties = keys.split(".");
        const type = dataType.split(".").pop();
        const len = properties.length;
        let value = object[properties[0]];
        if (len > 1) {
            for (let i = 1; i < len; i++) {
                value = value[properties[i]];
            }
        }

        if (type === "date") {
            return new Date(value).getTime()
        } else if (type === "string") {
            return value === "" || value === undefined ? "" : value.toUpperCase();
        } else {
            return value;
        }
    }
    _sortByName(e)
    {
        this._sort(e, 'name-header', 'fileName', 'string');
    }
    _sortByOwner(e)
    {
        this._sort(e, 'owner-header', 'owner.name', 'object.string');
    }
    _sortByExpiringDate(e)
    {
        this._sort(e, 'expiring-date-header', 'before', 'date');
    }
    _sortByActivities(e)
    {
        this._sort(e, 'activities-header', 'activity', 'string');
    }
    _sort(event, nodeName, key, dataType)
    {
        this.sort.isSorted = true;
        this.sort.columnKey = key;
        this.sort.type = dataType;
        if (this.$[nodeName].querySelector('iron-icon').classList.contains('header-icon-sorted')) {
            this.sharedFiles.reverse();
            this.$['dom-repeat'].items = [];
            this.$['dom-repeat'].items = this.sharedFiles;
            this.$[nodeName].querySelector('iron-icon').classList.toggle('sort');
        } else {
            this._compare(key, dataType);
            this.$['dom-repeat'].items = [];
            this.$['dom-repeat'].items = this.sharedFiles;
            this.$[nodeName].querySelector('iron-icon').classList.add('header-icon-sorted');
            this.$[nodeName].classList.add('sorting');
            const ids = ['name-header', 'owner-header', 'expiring-date-header', 'activities-header'];
            for (let i =0 ; i < 4; i++) {
                if (ids[i] !== nodeName &&
                    this.$[ids[i]].querySelector('iron-icon').classList.contains('header-icon')) {
                    this.$[ids[i]].querySelector('iron-icon').classList.replace('header-icon', 'none');
                    this.$[ids[i]].querySelector('iron-icon').classList.remove('header-icon-sorted');
                    this.$[ids[i]].classList.remove('sorting');
                }
            }
        }
        event.stopPropagation();
    }
    _overName()
    {
        this._over(this.$['name-header']);
    }
    _outName()
    {
        this._out(this.$['name-header']);
    }
    _overOwner()
    {
        this._over(this.$['owner-header']);
    }
    _outOwner()
    {
        this._out(this.$['owner-header']);
    }
    _overExpiringDate()
    {
        this._over(this.$['expiring-date-header']);
    }
    _outExpiringDate()
    {
        this._out(this.$['expiring-date-header']);
    }
    _overActivities()
    {
        this._over(this.$['activities-header']);
    }
    _outActivities()
    {
        this._out(this.$['activities-header']);
    }
    _over(node)
    {
        if (node.querySelector('iron-icon').classList.contains('none')) {
            node.querySelector('iron-icon').classList.replace('none', 'header-icon');
        }
    }
    _out(node)
    {
        if (node.querySelector('iron-icon').classList.contains('header-icon-sorted')) {
            return;
        }
        if (node.querySelector('iron-icon').classList.contains('header-icon')) {
            node.querySelector('iron-icon').classList.replace('header-icon', 'none');
        }
    }
    _getFileMetaData(item)
    {
        return new Promise((resolve, reject) => {
            const fileMetaDataWorker = new Worker('./scripts/tasks/file-metadata-task.js');
            fileMetaDataWorker.addEventListener('message', (e) => {
                resolve(e.data);
                fileMetaDataWorker.terminate();
            }, false);
            fileMetaDataWorker.addEventListener('error', (e)=> {
                fileMetaDataWorker.terminate();
                reject(e);
            }, false);
            this.authenticationParameters = {"scheme": "Bearer", "value": item.macaroon};
            fileMetaDataWorker.postMessage({
                'endpoint' : `${window.location.origin}${window.CONFIG['dcache-view.endpoints.webapi']}`,
                'file' : item,
                'filePath' : this.encodePath(item.filePath),
                'scope' : 'full',
                'limit' : 100,
                'upauth' : this.getAuthValue()
            });
        });
    }
    _appendFM(file)
    {
        this._getFileMetaData(file).then((fm) => {
            const index = this.sharedFiles.indexOf(file);
            //FIXME - this a very bad workaround - needs to be investigated
            this.set(`sharedFiles.${index}.fileMetaData`, fm);
            this.$.content.querySelectorAll('.row')[index].querySelector('file-icon').mimeType =
                fm.fileMimeType;
        }).catch(err => {
            //TODO - use expired style
            console.info(err.message);
        });
    }
    _select(e)
    {
        const initSelectedState = e.model.item.selected;
        this.set(`sharedFiles.${e.model.index}.selected`, !e.model.item.selected);
        if (this.multipleSelection) {
            if (initSelectedState) {
                const index = this.selectionMonitoring.findIndex((sf) => {
                    return sf.macaroon === e.model.item.macaroon;
                });
                this.splice('selectionMonitoring', index, 1);
            } else {
                this.push('selectionMonitoring', e.model.item);
            }
        } else {
            this.selectionMonitoring = [];
            this.sharedFiles.forEach((sf, i) => {
                if (sf.macaroon !== e.model.item.macaroon && sf.selected === true) {
                    this.set(`sharedFiles.${i}.selected`, false);
                }
            });
            if (!initSelectedState) {
                this.push('selectionMonitoring', e.model.item);
            }
        }
    }
    _computedClass(isSelected)
    {
        let classes = 'row';
        if (isSelected) {
            classes += ' selected';
        }
        this.updateStyles();
        return classes;
    }
    _clearSelection(event)
    {
        if (event.detail.element === "view-file"
            && event.type === "dv-namespace-reset-element-internal-parameters") {
            this.sharedFiles.forEach((shareFile, index) => {
                if (shareFile.selected === true) {
                    this.set(`sharedFiles.${index}.selected`, false);
                }
            });
            this.selectionMonitoring = [];
        }
    }
    _openFile(e)
    {
        const item = e.model.item || e.detail.item;

        if (this._isExpired(item.before)) {
            this.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast', {
                detail: {message: "You can not access this file anymore, the macaroon has expired."},
                bubbles: true, composed: true}));
            return;
        }
        if (item.fileMetaData.fileType) {
            this.dispatchEvent(
                new CustomEvent('dv-file-sharing-open-file', {
                    detail: {file: item}, bubbles: true, composed: true}));
        }
    }
    _openContextMenu(e)
    {
        if (!e.model.item.selected) {this._select(e);}
        e.preventDefault();
        e.stopPropagation();
        /**
         * A WORKAROUND for firefox
         * TODO: When firefox support web components fully, revisit.
         */
        this.dispatchEvent(new MouseEvent('contextmenu', {
            view: window,
            bubbles: true,
            composed: true,
            cancelable: true,
            clientX: e.clientX,
            clientY: e.clientY
        }));
    }
    _removeItems(event)
    {
        const files = event.detail.files;
        const len = this.sharedFiles.length;
        files.forEach( (file) => {
            for (let i=0; i < len; i++) {
                if (this.sharedFiles[i].macaroon === file.macaroon) {
                    const macaroons = sessionStorage.macaroonList.split(" ");
                    sessionStorage.macaroonList =
                        macaroons.filter(macaroon => macaroon !== file.macaroon).join(" ");
                    this.splice('sharedFiles', i, 1);
                    if (this.selectionMonitoring.length > 0) {
                        const idx = this.selectionMonitoring.indexOf(file);
                        this.splice('selectionMonitoring', idx, 1);
                    }
                    break;
                }
            }
        });
        this._update(this.sharedFiles.length);
    }
    _renameInputStart(e)
    {
        const index = this.sharedFiles.findIndex((file) => {
            return file.macaroon === e.detail.file.macaroon;
        });
        if (index > -1) {
            const node = this.$['content'].querySelectorAll('.row')[index];
            const listRow = node.querySelector('#fileName');
            const input = new RenameInput(e.detail.file, `Bearer ${e.detail.file.macaroon}`);
            this.removeAllChildren(listRow);
            listRow.appendChild(input);
        }
    }

    _renameInputEnd(e)
    {
        const index = this.sharedFiles.findIndex((file) => {
            return file.fileMetaData.pnfsId === e.detail.pnfsId;
        });
        if (index > -1) {
            const node = this.$['content'].querySelectorAll('.row')[index];
            const listRow = node.querySelector('#fileName');
            const span = document.createElement('span');
            const t = document.createTextNode(`${e.detail.newFileName}`);
            span.appendChild(t);
            this.removeAllChildren(listRow);
            listRow.appendChild(t);
            this.sharedFiles[index].fileName = e.detail.newFileName;
            this.sharedFiles.notifyPath(`sharedFiles.${index}.fileName`);
        }
    }
}
window.customElements.define(SharedFileList.is, SharedFileList);