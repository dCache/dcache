'use strict';

/**
 * --- Adopted from ----
 * https://github.com/googledrive/cors-upload-sample/blob/master/upload.js
 *
 * A class that can be use to upload any file into dcache.
 *
 * @constructor
 * @param {object} options Hash of options
 * @param {string} options.upauth Accept both Basic and Bearer. If it is Basic; Encoded Base64 of username:password
 * @param {file} options.file Blob-like item to upload
 * @param {url} options.url
 * @param {string} [options.contentType] Content-type, if overriding the type of the blob.
 * @param {function} [options.onComplete] Callback for when upload is complete
 * @param {function} [options.onProgress] Callback for status for the in-progress upload
 * @param {function} [options.onError] Callback if upload fails
 *
 *  @example
 *  var content = new Blob(["Hello world"], {"type": "text/plain"});
 *  var uploader = new UploadHandler({
 *      file: content,
 *      upauth: xxxx,
 *      onComplete: function(data) { ... },
 *      onError: function(data) { ... }
 *  })
 *  uploader.upload();
 */
const UploadHandler = function (options) {
    const noop = function () {
    };
    this.file = options.file;
    this.contentType = options.contentType || this.file.type || 'application/octet-stream';
    this.upauth = options.upauth;
    this.onComplete = options.onComplete || noop;
    this.onProgress = options.onProgress || noop;
    this.onError = options.onError || noop;
    this.url = options.url;
    this.httpMethod = 'PUT';
};

/**
 * Upload the actual file content.
 */
UploadHandler.prototype.upload = function()
{
    const content = this.file;
    const xhr = new XMLHttpRequest();
    xhr.open(this.httpMethod, this.url, true);
    xhr.withCredentials = true;
    xhr.setRequestHeader('Content-Type', this.contentType);
    if (this.upauth && this.upauth !=="") {
        xhr.setRequestHeader('Authorization', this.upauth);
    }
    xhr.setRequestHeader('Suppress-WWW-Authenticate', "Suppress");
    if (xhr.upload) {
        xhr.upload.addEventListener('progress', this.onProgress);
    }
    xhr.onload = this.onContentUploadSuccess_.bind(this);
    xhr.onerror = this.onContentUploadError_.bind(this);
    xhr.send(content);
};

/**
 * Handle successful responses for uploads. If complete, invokes
 * the caller's callback.
 *
 * @private
 * @param {object} e XHR event
 */
UploadHandler.prototype.onContentUploadSuccess_ = function(e)
{
    if (e.target.status === 200 || e.target.status === 201) {
        this.onComplete(this.file);
    } else {
        this.onContentUploadError_(e);
    }
};

/**
 * Handles errors for uploads. [Client Side] error.
 *
 * @private
 * @param {object} e XHR event
 */
UploadHandler.prototype.onContentUploadError_ = function(e)
{
    if (e.target.status && e.target.status < 500) {
        this.onError(e.target);
    } else {
        this.onError(e.target);
    }
};