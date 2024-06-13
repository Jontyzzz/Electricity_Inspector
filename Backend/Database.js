const mysql = require("mysql2/promise");
const loggers = require('../Backend/loggers/loggers');

class Database {
    constructor() { }

    async runQuery(query = "", parameters = []) {
        let pool;

        try {
            // Use a connection pool for better performance and management
            pool = await mysql.createPool({
                connectionLimit: 10,
                host: process.env.DB_HOST || '103.195.185.168',
                user: process.env.DB_USER || 'indiscpx_PVP',
                password: process.env.DB_PASSWORD || 'indiscpx_BLVL@123',
                database: process.env.DB_DATABASE || 'indiscpx_PVP'
            });

            loggers.dataLogger.log('info', `Connection pool created`);

            const [rows, fields] = await pool.execute(query, parameters);
            loggers.dataLogger.log('info', `Query is ${query} and parameters are ${parameters}`);
            return rows;
        } catch (err) {
            loggers.ErrorLogger.log('error', err);
            throw err; // Rethrow the error for the calling code to handle
        } finally {
            try {
                if (pool) {
                    // Close the connection pool
                    await pool.end();
                    loggers.dataLogger.log('info', `Connection pool closed`);
                }
            } catch (err) {
                loggers.ErrorLogger.log('error', `Error while closing connection pool ${err}`);
            }
        }
    }
}

module.exports = Database;
