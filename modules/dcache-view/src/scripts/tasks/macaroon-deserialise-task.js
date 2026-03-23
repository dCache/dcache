"use strict";
/**
 * @param {
 *     macaroon: @type {String}
 * }
 */
self.importScripts('../lazy-loads/macaroon-deserialiser.js');
self.addEventListener('message', function(e) {
    const list = e.data.macaroons;
    const len = list.length;
    const sharedFileslist = [];
    for (let i = 0; i<len; i++) {
        const macaroon = new MacaroonsDeSerialiser().deserialise(list[i]);
        const sharedFile = {"fileMetaData" : {}, "selected": false};
        for (let ai of macaroon.caveatPackets) {
            const caveat = ai.getValueAsText();
            const o = caveat.indexOf(":");
            if (caveat.substring(0, o) === "id") {
                const sharerInfo = caveat.substring(o + 1).trim().split(";");
                sharedFile["owner"] = {"uid": sharerInfo[0], "gid": sharerInfo[1], "name": sharerInfo[2]}
            } else if (caveat.substring(0, o) === "path") {
                const a =caveat.substring(o + 1).trim();
                sharedFile["fileName"] = a === "/" ? "Root" : a.substring(a.lastIndexOf("/") + 1);
                sharedFile["filePath"] = caveat.substring(o + 1).trim();
            } else {
                sharedFile[caveat.substring(0, o)] = caveat.substring(o + 1).trim();
            }
            sharedFile["macaroon"] = list[i];
        }
        sharedFileslist.push(sharedFile);
    }
    self.postMessage(sharedFileslist);
}, false);