import React, { useState, useEffect } from 'react';
import './Demo.css';

const MyComponent = () => {
  const [time, setTime] = useState(new Date());

  useEffect(() => {
    const intervalId = setInterval(() => {
      setTime(new Date());
    }, 1000);

    return () => clearInterval(intervalId);
  }, []);

  return (
    <div className="digital-watch">
      <div className="digit">
        {('0' + time.getHours()).slice(-2)}:
      </div>
      <div className="digit">
        {('0' + time.getMinutes()).slice(-2)}:
      </div>
      <div className="digit">
        {('0' + time.getSeconds()).slice(-2)}
      </div>
    </div>
  );
};

export default MyComponent;
