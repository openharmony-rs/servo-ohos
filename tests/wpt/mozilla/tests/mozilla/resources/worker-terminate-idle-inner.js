onmessage = e => {
  const port = e.data;
  setInterval(() => port.postMessage("tick"), 20);
};
