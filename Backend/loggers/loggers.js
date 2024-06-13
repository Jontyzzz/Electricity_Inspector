const {createLogger,transports,format, transport} = require('winston')

// logging  function ///

const dataLogger = createLogger({
    transports:[
        new transports.File({
            filename:'data.Log',
            level:'info',
            format: format.combine(format.timestamp(),format.json())   
        }),
       
    ]
})

// Logging function for socket logs
const socketLogger = createLogger({
    transports: [
        new transports.File({
            filename: 'socket.log',
            level: 'info',
            format: format.combine(format.timestamp(), format.json())
        })
    ]
});
//Logging function for Error logs
const ErrorLogger = createLogger({
    transports: [
        new transports.File({
            filename:'ErrorLogger.log',
            level:'error',
            format: format.combine(format.timestamp(),format.json())  
        }),
    ]
});

module.exports = {dataLogger,socketLogger,ErrorLogger}