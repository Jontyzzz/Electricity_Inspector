const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const mysql = require('mysql2');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

const dbConfig = {
    connectionLimit: 10,
    host: process.env.DB_HOST || '103.195.185.168',
    user: process.env.DB_USER || 'indiscpx_BLVL',
    password: process.env.DB_PASSWORD || 'indiscpx_BLVL@123',
    database: process.env.DB_DATABASE || 'indiscpx_BLVL'
};

// WebSocket event listener for connection
io.on('connection', (socket) => {
    console.log('A user connected');
    
    // WebSocket event listener for disconnection
    socket.on('disconnect', () => {
        console.log('A user disconnected');
    });
});

// Sample endpoint to serve your React app
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

// Your route handling logic for fetching data
app.get('/api/fetchData', validateAuth, async (req, res) => {
    let pool;
    try {
        pool = await mysql.createPool(dbConfig);
        const connection = await pool.getConnection();
        const columns = ['TT1', 'TT2', 'TT3', 'TT4', 'TT5', 'TT6', 'TT7', 'DateTime'];
        const [rows, fields] = await connection.query(`SELECT ?? FROM ONGC_IOT`, [columns]);

        // Emit 'dataUpdate' event to all connected clients
        io.emit('dataUpdate', rows);

        res.json(rows);
    } catch (error) {
        console.error('Error fetching data:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        try {
            if (pool) pool.end();
        } catch (err) {
            console.error('Error releasing pool:', err);
        }
    }
});

// Start the server
const PORT = process.env.PORT || 9000;
server.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
