
const express = require("express");
const mysql = require("mysql2/promise");
const cors = require("cors");
const path = require('path');
const Database = require("./Database");
const jwt = require('jsonwebtoken');
const bodyParser = require('body-parser');
const http = require('http');
const { Server } = require('socket.io');
const loggers = require('../Backend/loggers/loggers');
const { validateAuth } = require('./JWT/middlewareauth');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const port = process.env.PORT || 9000;

const dbConfig = {
  connectionLimit: 10,
  host: process.env.DB_HOST || '103.195.185.168',
  user: process.env.DB_USER || 'indiscpx_PVP',
  password: process.env.DB_PASSWORD || 'indiscpx_BLVL@123',
  database: process.env.DB_DATABASE || 'indiscpx_PVP'
};

// Function to decode JWT token
const decodeJwtToken = (token) => {
  if (token) {
      try {
          const payloadBase64 = token.split('.')[1];
          const decodedPayload = JSON.parse(atob(payloadBase64));
          return decodedPayload;
      } catch (error) {
          console.error('Error decoding JWT token:', error);
          return null;
      }
  } else {
      console.error('Token is undefined or null');
      return null;
  }
};



io.on("error", (error) => {
  console.error("Socket.io server error:", error);
});

app.use(cors());
app.use(express.json());
app.use(bodyParser.json());
app.get('/api/getdata', validateAuth, async (req, res) => {
  try {
    let date = req.query.date;
    let sql = "SELECT * FROM `HRN_ONGC_UPDATE` WHERE date = ?";
    let data = await (new Database()).runQuery(sql, [date]);

    // Emit a 'dataUpdate' event to all connected clients
    io.emit('dataUpdate', data);

    loggers.socketLogger.log('info', 'Emitting dataUpdate event to clients');
  
    return res.json(data);
  } catch (error) {
    console.error('Error fetching data:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// app.get('/api/fetchData', async (req, res) => {
//   let pool, connection;

//   try {
//     pool = await mysql.createPool(dbConfig);
//     connection = await pool.getConnection();
//     const columns = ['TT1', 'TT2', 'TT3', 'TT4', 'TT5', 'TT6', 'TT7', 'DateTime'];
//     const [rows, fields] = await connection.query(`SELECT ?? FROM ONGC_IOT`, [columns]);
//     res.json(rows);
//   } catch (error) {
//     console.error('Error fetching data:', error);
//     res.status(500).json({ error: 'Internal Server Error' });
//   } finally {
//     try {
//       if (pool && connection) {
//         connection.release();
//       }
//     } catch (err) {
//       console.error('Error releasing connection:', err);
//     }
//   }
// });


app.get('/api/fetchData', async (req, res) => {
  let pool, connection;

  try {
    pool = await mysql.createPool(dbConfig);
    connection = await pool.getConnection();

    // Get the date from the frontend datepicker
    const selectedDate = req.query.selectedDate; // Assuming the frontend sends the selected date as a query parameter
    
    // Construct the SQL query to fetch unique hourly data for the selected date
    const sqlQuery = `
      SELECT 
        DATE_FORMAT(DateTime, '%Y-%m-%d %H:00:00') AS HourlyDateTime,
        AVG(TT1) AS TT1,
        AVG(TT2) AS TT2,
        AVG(TT3) AS TT3,
        AVG(TT4) AS TT4,
        AVG(TT5) AS TT5,
        AVG(TT6) AS TT6,
        AVG(TT7) AS TT7
      FROM 
        ONGC_IOT 
      WHERE 
        DATE(DateTime) = ?
      GROUP BY 
        HourlyDateTime
    `;

    // Execute the SQL query with the selected date
    const [rows, fields] = await connection.query(sqlQuery, [selectedDate]);
    res.json(rows);
    // console.log(`${selectedDate} hourly data fetched..`);
  } catch (error) {
    console.error('Error fetching hourly data:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  } finally {
    try {
      if (pool && connection) {
        connection.release();
      }
    } catch (err) {
      console.error('Error releasing connection:', err);
    }
  }
});

app.get('/api/fetchMinutelyData', async (req, res) => {
  let pool, connection;

  try {
    pool = await mysql.createPool(dbConfig);
    connection = await pool.getConnection();

    // Get the date from the frontend datepicker
    const selectedDate = req.query.selectedDate; // Assuming the frontend sends the selected date as a query parameter
    
    // Construct the SQL query to fetch unique minutely data for the selected date
    const sqlQuery = `
      SELECT 
        DATE_FORMAT(DateTime, '%Y-%m-%d %H:%i:00') AS MinutelyDateTime,
        AVG(TT1) AS TT1,
        AVG(TT2) AS TT2,
        AVG(TT3) AS TT3,
        AVG(TT4) AS TT4,
        AVG(TT5) AS TT5,
        AVG(TT6) AS TT6,
        AVG(TT7) AS TT7
      FROM 
        ONGC_IOT 
      WHERE 
        DATE(DateTime) = ?
      GROUP BY 
        MinutelyDateTime
    `;

    // Execute the SQL query with the selected date
    const [rows, fields] = await connection.query(sqlQuery, [selectedDate]);
    res.json(rows);
    // console.log(`${selectedDate} minutely data fetched..`);
  } catch (error) {
    console.error('Error fetching minutely data:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  } finally {
    try {
      if (pool && connection) {
        connection.release();
      }
    } catch (err) {
      console.error('Error releasing connection:', err);
    }
  }
});


app.post('/api/signup', async (req, res) => {
  let sql = "INSERT INTO login (`name`, `email`, `password`) VALUES (?,?,?)";
  let values = [req.body.name, req.body.email, req.body.password];
  let data = await (new Database()).runQuery(sql, values);
  return res.json(data);
});

app.post('/api/login', async (req, res) => {
  let sql = "SELECT Role FROM login WHERE email = ? AND password = ?";
  let values = [req.body.email, req.body.password];
  
  try {
    let data = await (new Database()).runQuery(sql, values);
    
    if (data.length > 0) {
      const role = data[0].Role; 
      const token = jwt.sign({ email: req.body.email, role }, "ONGC", { expiresIn: '1h' });
      return res.json({ isLogged: "success", token, isAdmin: role === 'admin' });
    } else {
      return res.status(401).json({ error: "Invalid email or password" });
    }
  } catch (error) {
    console.error('Error:', error);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});



// Define the POST endpoint for updating status
app.post('/api/updateStatus', async (req, res) => {
  // Extract email and newStatus from request body
  const { email, newStatus } = req.body;

  // Define pool and connection variables
  let pool, connection;

  try {
    // Create a connection pool
    pool = await mysql.createPool(dbConfig);

    // Get a connection from the pool
    connection = await pool.getConnection();

    // Update the status in the database
    const query = 'UPDATE login SET status = ? WHERE email = ?';
    const [results] = await connection.query(query, [newStatus, email]);

    console.log('Status updated successfully');
    res.json({ message: 'Status updated successfully' });
  } catch (error) {
    console.error('Error updating status:', error);
    res.status(500).json({ error: 'Error updating status' });
  } finally {
    // Release the connection back to the pool
    if (connection) {
      connection.release();
    }
  }
});


// employee page api
app.get('/api/employees', async (req, res) => {
  const { email } = req.query;
  try {
    const pool = await mysql.createPool(dbConfig);
    const connection = await pool.getConnection();
    const [rows, fields] = await connection.query('SELECT name, email FROM login WHERE email = ?', [email]);
    if (rows.length > 0) {
      const user = rows[0];
      res.json(user);
    } else {
      res.status(404).json({ error: 'User not found' });
    }
    connection.release();
  } catch (error) {
    console.error('Error executing MySQL query:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/api/getUserRole', async (req, res) => {
  try {
    // Extract the token from the Authorization header
    const token = req.headers.authorization;

    if (!token || !token.startsWith('Bearer ')) {
      return res.status(400).json({ error: 'Invalid or missing token' });
    }

    // Extract the JWT token from the Authorization header
    const tokenParts = token.split(' ');
    const jwtToken = tokenParts[1];

    // Decode the JWT token to get the user's email
    const decodedToken = decodeJwtToken(jwtToken);

    if (!decodedToken || !decodedToken.email) {
      return res.status(400).json({ error: 'Invalid token or missing email' });
    }

    // Query the database to retrieve the user's role based on their email
    const email = decodedToken.email;
    const sql = "SELECT Role FROM login WHERE email = ?";
    const [results, fields] = await pool.query(sql, [email]); // Destructure results and fields

    // Check if the user was found
    if (results.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Return the user's role
    const role = results[0].Role;
    res.json({ role });
  } catch (error) {
    console.error('Error fetching user role:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});



// Admin api to fetch data//
  app.get('/api/Admin', async (req, res) => {
    let pool, connection;

    try {
      pool = await mysql.createPool(dbConfig);
      connection = await pool.getConnection();
      const [rows, fields] = await connection.query('SELECT id, name, email, password, Role, status FROM login');
      res.json(rows);
    } catch (error) {
      console.error('Error executing MySQL query:', error);
      res.status(500).json({ error: 'Internal Server Error' });
    } finally {
      try {
        if (pool && connection) {
          connection.release();
        }
      } catch (err) {
        console.error('Error releasing connection:', err);
      }
    }
  });
// Route to handle DELETE requests to delete a user
app.delete('/api/users/:id', async (req, res) => {
  const userId = req.params.id; // Get the user ID from the request URL

  let pool, connection;

  try {
    // Create a MySQL connection pool using the provided database configuration
    pool = await mysql.createPool(dbConfig);
    // Get a connection from the pool
    connection = await pool.getConnection();

    // Execute the DELETE query to remove the user with the specified ID
    const [result] = await connection.query('DELETE FROM login WHERE id = ?', [userId]);

    // Check if any rows were affected by the query
    if (result.affectedRows === 0) {
      // If no rows were affected, the user with the specified ID was not found
      return res.status(404).json({ error: 'User not found' });
    }

    // Respond with a success message
    res.json({ message: 'User deleted successfully' });
  } catch (error) {
    // If an error occurs, log the error and respond with a 500 Internal Server Error
    console.error('Error executing MySQL query:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  } finally {
    // Release the connection back to the pool
    try {
      if (pool && connection) {
        connection.release();
      }
    } catch (err) {
      console.error('Error releasing connection:', err);
    }
  }
});


// Route to retrieve user role based on email
const pool = mysql.createPool(dbConfig);
// app.get('/api/getUserRole', async (req, res) => {
//   try {
//     // Extract the token from the Authorization header
//     const token = req.headers.authorization;

//     // Decode the JWT token to get the user's email
//     const decodedToken = decodeJwtToken(token);

//     if (!decodedToken || !decodedToken.email) {
//       return res.status(400).json({ error: 'Invalid token or missing email' });
//     }

//     // Query the database to retrieve the user's role based on their email
//     const email = decodedToken.email;
//     const sql = "SELECT Role FROM login WHERE email = ?";
//     pool.query(sql, [email], (error, results) => {
//       if (error) {
//         console.error('Error fetching user role:', error);
//         return res.status(500).json({ error: 'Internal Server Error' });
//       }

//       // Check if the user was found
//       if (results.length === 0) {
//         return res.status(404).json({ error: 'User not found' });
//       }

//       // Return the user's role
//       const role = results[0].Role;
//       res.json({ role });
//     });
//   } catch (error) {
//     console.error('Error fetching user role:', error);
//     res.status(500).json({ error: 'Internal Server Error' });
//   }
// });
app.get('/api/getUserRole', async (req, res) => {
  try {
    // Extract the token from the Authorization header
    const token = req.headers.authorization;

    // Decode the JWT token to get the user's email
    const decodedToken = decodeJwtToken(token);

    if (!decodedToken || !decodedToken.email) {
      return res.status(400).json({ error: 'Invalid token or missing email' });
    }

    // Extract email from decoded token
    const email = decodedToken.email;

    // Query the database to retrieve the user's role based on their email
    const sql = "SELECT Role FROM login WHERE email = ?";
    pool.query(sql, [email], (error, results) => {
      if (error) {
        console.error('Error fetching user role:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
      }

      // Check if the user was found
      if (results.length === 0) {
        return res.status(404).json({ error: 'User not found' });
      }

      // Return the user's role
      const role = results[0].Role;
      res.json({ role });
    });
  } catch (error) {
    console.error('Error fetching user role:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

////Update
// Update user password endpoint
app.put('/api/users/:id/password', async (req, res) => {
  const userId = req.params.id;
  const newPassword = req.body.newPassword;

  let connection;

  try {
    // Create a MySQL connection using the provided database configuration
    const pool = mysql.createPool(dbConfig);

    // Get a connection from the pool
    connection = await pool.getConnection();

    // Execute the SQL query to update the user's password
    const [result] = await connection.execute(
      'UPDATE login SET password = ? WHERE id = ?',
      [newPassword, userId]
    );

    // Check if any rows were affected
    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Password updated successfully
    res.json({ message: 'Password updated successfully' });
  } catch (error) {
    console.error('Error updating password:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  } finally {
    // Release the connection back to the pool
    if (connection) {
      connection.release();
    }
  }
});




io.on("connection", (socket) => {
  console.log('A client connected');
  socket.on('disconnect', () => {
    console.log('Client disconnected');
    // Additional logic for socket closure if needed
  });
});


server.listen(port, () => {
  console.log(`Server Started On ${port}`);
});
