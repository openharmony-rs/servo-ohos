let nested;
onmessage = e => {
  nested = new Worker("worker-terminate-idle-inner.js");
  nested.postMessage(e.data, [e.data]);
  postMessage("started");
};
