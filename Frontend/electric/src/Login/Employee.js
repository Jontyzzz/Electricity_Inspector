import React, { useState, useEffect } from 'react';
import axios from 'axios';
import Navbar from '../Navbar/Navbar';
import './Employee.css';

// Custom function to decode JWT token
// Custom function to decode JWT token
const decodeJwtToken = (token) => {
  try {
    const payloadBase64 = token.split('.')[1];
    const decodedPayload = JSON.parse(atob(payloadBase64));
    return decodedPayload;
  } catch (error) {
    console.error('Error decoding JWT token:', error);
    return null;
  }
};



function Employee() {
  // State to store the fetched data
  const [userData, setUserData] = useState([]);
  
  useEffect(() => {
    const fetchData = async () => {
      try {
        const token = localStorage.getItem('token');
        const decodedToken = decodeJwtToken(token); // Decode the JWT token
        console.log(decodedToken)
        if (decodedToken && decodedToken.email) {
          const userEmail = decodedToken.email;
          const response = await axios.get(`http://localhost:9000/api/employees?email=${userEmail}`, {
            headers: { Authorization: `Bearer ${token}` },
          });
          setUserData([response.data]); // Update state with the fetched user data
        } else {
          console.error('Invalid token or missing email information');
        }
      } catch (error) {
        console.error('Error fetching data:', error);
      }
    };

    fetchData();
  }, []);

  return (
    <div>
      <Navbar />
      <span >
        <h1 className="MY_PROFILE">Employee Profile</h1>
      </span>
      <div className="cont_ainer">
      
        <form>
        <div className='FBH'>
          {userData.map((user, index) => (
            <div key={index} className="form-group">
              <label className='label_FW' htmlFor={`name${index}`}>Name:</label>
              <input className='INPB' type="text" id={`name${index}`} name={`name${index}`} value={user.name} readOnly />
              <label className='label_FW' htmlFor={`email${index}`}>Email:</label>
              <input  className='INPB' type="email" id={`email${index}`} name={`email${index}`} value={user.email} readOnly />
            </div>
            
          ))}
          </div>
        </form>
      </div>
    </div>
  );
}
export default Employee;

