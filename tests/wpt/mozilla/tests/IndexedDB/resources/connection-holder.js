// Holds a connection to a database for the page that started this worker. Messages: {name, close}
// opens the database named name and reports 'opened', then its versionchange events, closing the
// connection on them if close is set; 'close' closes the connection and reports 'closed'.
'use strict';

let db;

function handle(message, post) {
  if (message === 'close') {
    db.close();
    post('closed');
    return;
  }
  const request = indexedDB.open(message.name);
  request.onsuccess = () => {
    db = request.result;
    db.onversionchange = event => {
      post(`versionchange ${event.oldVersion} ${event.newVersion}`);
      if (message.close) {
        db.close();
      }
    };
    post('opened');
  };
  request.onerror = () => post(`error ${request.error.name}`);
}

if ('onconnect' in self) {
  self.onconnect = event => {
    const port = event.ports[0];
    port.onmessage = message => handle(message.data, data => port.postMessage(data));
  };
} else {
  self.onmessage = message => handle(message.data, data => self.postMessage(data));
}
