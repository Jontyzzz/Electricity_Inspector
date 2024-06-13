// import React, { useEffect, useState } from 'react';
// import jwt from 'jsonwebtoken';
// import axios from 'axios';
// import { useHistory } from 'react-router-dom';

// function AuthenticatedComponent() {
//   const [user, setUser] = useState(null);
//   const history = useHistory();

//   useEffect(() => {
//     // Retrieve the token from localStorage
//     const storedToken = localStorage.getItem('token');

//     const fetchUserRole = async (email) => {
//       try {
//         const response = await axios.get(`/api/getUserRole?email=${email}`, {
//           // Add any necessary headers, like Authorization with the token
//         });

//         const role = response.data.role;

//         // Check the role and navigate accordingly
//         if (role === 'admin') {
//           history.push('/admin');
//         } else if (role === 'employee') {
//           history.push('/employee');
//         } else {
//           // Unknown or unsupported role
//           console.error('Unknown or unsupported role:', role);
//         }
//       } catch (error) {
//         console.error('Error fetching user role:', error);
//       }
//     };

//     if (storedToken) {
//       // Decode the token
//       try {
//         const decodedToken = jwt.decode(storedToken);

//         // Verify that the token is valid and contains necessary information
//         if (decodedToken && decodedToken.email) {
//           setUser(decodedToken);

//           // If the token doesn't contain the role, fetch it from the server
//           if (!decodedToken.role) {
//             fetchUserRole(decodedToken.email);
//           }
//         } else {
//           // Invalid token or missing required information
//           console.error('Invalid token or missing required information');
//         }
//       } catch (error) {
//         console.error('Error decoding token:', error.message);
//       }
//     }
//   }, [history]);
// }

// export default AuthenticatedComponent;
