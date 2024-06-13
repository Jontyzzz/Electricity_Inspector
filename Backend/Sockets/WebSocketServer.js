const http = require('http');
const WebSocket = require('ws');

const server = http.createServer();
const wss = new WebSocket.Server({ server });

const port = 9090;

wss.on('connection', (ws) => {
  console.log('A client connected');

  // Listen for messages from clients
  ws.on('message', (message) => {
    console.log(`Received message: ${message}`);

    // You can add custom logic to handle the received message

    // Example: Broadcast the message to all connected clients
    wss.clients.forEach((client) => {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  });

  // Send a welcome message to the connected client
  ws.send('Welcome to the WebSocket server!');
});


server.listen(port, () => {
  console.log(`WebSocket Server started on port ${port}`);
});
