/**
 * @param {
 *     endpoint: @type {String}
 *          url for the rest api
 *          @default `./api/v1/`
 *
 *     scope: @type {String}
 *          @value `partial` || `full`
 *          @default `partial`
 *
 *     upauth: @type {String}
 *          if set and non-empty, the Authorization header
 *          will be set with this value.
 *
 *     limit: @type {String|Number}
 *          Limit number of replies in directory listing.
 *          @value number || `max`. if max, the value is set to 2147483647
 *          @default `100`
 *
 *     offset: @type {String|Number}
 *          Number of entries to skip in directory listing.
 *          @default `0`
 *
 *     file: @type {Object}
 *          @param {
 *              pnfsId: @type {Number}
 *          }
 *
 *     filePath: @type {String}
 *          absolute path of the file location.
 * }
 */

self.addEventListener('message', function (e) {
  const endpoint = e.data.endpoint ? e.data.endpoint : "./api/v1/";
  const detail = e.data.scope ? e.data.scope : "partial";
  const headers = new Headers({
    "Suppress-WWW-Authenticate": "Suppress",
    "Accept": "application/json",
    "Content-Type": "application/json"
  });

  const qos = e.data.upauth === "Basic YW5vbnltb3VzOm5vcGFzc3dvcmQ=" ? "false"
      : "true";
  const limit = e.data.limit ? e.data.limit === 'max' ? 2147483647
      : e.data.limit : 100;
  const offset = e.data.offset ? e.data.offset : 0;
  const pnfsID = e.data.file && e.data.file.pnfsId ? e.data.file.pnfsId
      : undefined;

  let partialFileMetadata;
  if (e.data.upauth && e.data.upauth !== "") {
    headers.append("Authorization", `${e.data.upauth}`);
  }

  if (!pnfsID && e.data.filePath) {
    //no pnfsID
    partial(e.data.filePath).then((response) => {
      partialFileMetadata = response;
      if (detail === "partial") {
        self.postMessage(response);
      } else {
        full(response.pnfsId).then((response) => {
          self.postMessage(response);
        });
      }
    }).catch((err) => {
      /**
       * WORKAROUND:
       * https://stackoverflow.com/questions/30715367/why-can-i-not-throw-inside-a-promise-catch-handler
       */
      setTimeout(function () {
        throw err;
      });
    })

  } else if (e.data.file.pnfsId) {
    if (detail === "partial") {
      partial(e.data.filePath).then((response) => {
        self.postMessage(response);
      }).catch((err) => {
        setTimeout(function () {
          throw err;
        });
      })
    } else {
      full(e.data.file.pnfsId).then((response) => {
        self.postMessage(response);
      }).catch((err) => {
        setTimeout(function () {
          throw err;
        });
      });
    }
  } else {
    throw new TypeError(
        "The file object parameter is not set. Provide either file.pnfsId or filePath.");
  }
  function full(pnfsId) {
    return fetchRequest(
        new Request(`${endpoint}id/${pnfsId}`, {headers: headers})
    ).catch((err) => {
      if (partialFileMetadata) {
        return partialFileMetadata;
      }
      throw err;
    });
  }

  function partial(path) {
    return fetchRequest(new Request(
        `${endpoint}labels${path}?labels=true&offset=${offset}&limit=${limit}`,
        {
          headers: headers
        }));

  }

  function fetchRequest(req) {
    return fetch(req).then((response) => {

      if (!response.ok) {
        throw JSON.stringify(
            {status: response.status, message: response.statusText})
      }

      return response.json();
    });
  }
}, false);