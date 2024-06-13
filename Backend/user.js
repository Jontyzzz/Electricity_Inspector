const jwt = require('jsonwebtoken');
const express = require('express');
const app = express();

// Middleware to authenticate the token
function authenticateToken(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (token == null) return res.sendStatus(401);

    // Verify the token
    jwt.verify(token, process.env.TOKEN_SECRET, (err, user) => {
        console.log(err);

        if (err) return res.sendStatus(403);

        // Attach the user to the request object for further use
        req.user = user;

        next();
    });
}

// Function to generate an access token
function generateAccessToken(username) {
    // Replace 'your-secret-key' with your actual secret key
    return jwt.sign({ username }, process.env.TOKEN_SECRET, { expiresIn: '1800s' });
}

// Protected route that requires authentication
app.get('/api/protected', authenticateToken, (req, res) => {
    // Access the authenticated user through req.user
    res.json({ message: 'Protected route accessed', user: req.user });
});

// Function to generate a JSON Web Token
function generateJWT(user) {
    // Replace 'your-secret-key' with your actual secret key
    const secretKey = '279918F8D349243669B428C6973B0733CAFB8F4D6D47EC79DE00FC2DD83DF3E7';

    // Define the payload for the token
    const payload = {
        user,
        // You can add more information to the payload if needed
    };

    // Create and sign the token
    const token = jwt.sign(payload, secretKey, { expiresIn: '1h' }); // Set expiration time

    return token;
}
