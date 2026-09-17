if (!window.DcacheViewMixins) {
  window.DcacheViewMixins = {};
}

DcacheViewMixins.LABELS =  Polymer.dedupingMixin((base) =>
{
  return class extends base {
    fileMetadataPromise(message)
    {

      return new Promise((resolve, reject) => {
        const labelWorker = new Worker('./scripts/tasks/label-task.js');

        labelWorker.addEventListener('message', (e) => {
          resolve(e.data);
          labelWorker.terminate();
        }, false);
        labelWorker.addEventListener('error', (err) => {
          reject(err);
          labelWorker.terminate();
        }, false);

        labelWorker.postMessage(message);
      });
    }
  }
});