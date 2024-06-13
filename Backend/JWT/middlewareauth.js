const { getUser } = require('..//JWT/Auth.controller')
const jwt = require('jsonwebtoken')

// middeleware used for authentication handler

async function restrictToLoggedinUserOnly(req, res, next) {
    const userUid = req.cookies.Uid;

    if (!userUid) return res.redirect('/login')
    const user = getUser(userUid)

    if (!user) return res.redirect("/login");

    req.user = user;
    // next("/Home");
    next();
}

function validateAuth(req, res, next) {
    const token = req.headers.authorization && req.headers.authorization.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'Token missing' });
    jwt.verify(token, "ONGC", (err, decoded) => {
        if (err) return res.status(403).json({ error: 'Invalid token' });
    });
    next()
}

module.exports = { restrictToLoggedinUserOnly,validateAuth};