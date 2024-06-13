
import React, { useEffect, useState } from "react";
import Navbar from "../Navbar/Navbar";
import axios from "axios";
import "./Admin.css";
import Popup from "reactjs-popup";
import "reactjs-popup/dist/index.css";
import { RiCloseCircleFill } from "react-icons/ri";
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faEye, faEyeSlash } from '@fortawesome/free-solid-svg-icons';

function Admin() {
  const [userData, setUserData] = useState([]);
  const [passwordVisibility, setPasswordVisibility] = useState({});
  const [decodedToken, setDecodedToken] = useState(null);
  const [emailInput, setEmailInput] = useState("");
  const [showPassword, setShowPassword] = useState(false); // State to track password visibility


  const togglePasswordVisibility = () => {
    setShowPassword(prevState => !prevState);
};

  // Function to fetch user data
  const fetchData = async () => {
    try {
      const token = localStorage.getItem("token");
      const response = await axios.get("http://localhost:9000/api/Admin", {
        headers: { Authorization: `Bearer ${token}` },
      });
      setUserData(response.data);

      // Decode the token and store the decoded data into the state
      const decodedData = decodeToken(token);
      setDecodedToken(decodedData);

      // Set email for input box
      setEmailInput(decodedData.email);

      console.log(response.data);
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  };

  useEffect(() => {
    fetchData();
  }, []); //

  // Function to decode the token
  const decodeToken = (token) => {
    try {
      const base64Url = token.split(".")[1];
      const base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
      const jsonPayload = decodeURIComponent(
        atob(base64)
          .split("")
          .map((c) => {
            return "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2);
          })
          .join("")
      );
      return JSON.parse(jsonPayload);
    } catch (error) {
      console.error("Error decoding token:", error);
      return null;
    }
  };


  // const togglePasswordVisibility = (userId) => {
  //   setPasswordVisibility((prevState) => ({
  //     ...prevState,
  //     [userId]: !prevState[userId],
  //   }));
  // };

  const deleteUser = async (userId) => {
    try {
      const token = localStorage.getItem("token");
      await axios.delete(`http://localhost:9000/api/users/${userId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      // After deletion, fetch updated user data
      fetchData();
    } catch (error) {
      console.error("Error deleting user:", error);
    }
  };

 const [newPassword, setNewPassword] = useState(""); // State to hold the new password input

  const handlePasswordUpdate = async (userId) => {
    try {
      const token = localStorage.getItem("token");
      const response = await axios.put(`/api/users/${userId}/password`, { newPassword }, {
        headers: { Authorization: `Bearer ${token}` },
      });
      console.log(response.data); // Log the response from the server
      alert("Password updated successfully");
      // Fetch updated user data after password update
      fetchData();
    } catch (error) {
      console.error("Error updating password:", error);
      alert("Error updating password");
    }
  };
  
  

  return (
    <div style={{ backgroundColor: "whitesmoke" }}>
      <meta
        name="viewport"
        content="width=device-width, initial-scale=1"
      ></meta>
      <Navbar />
      <span>
        <h1 className="MY_ADMIN">Admin Profile</h1>
      </span>
      <div className="CTC col-8">
        <table className="CT">
          <thead className="TTHD">
            <tr className="CTH">
              <th className="ATH">ID</th>
              <th className="ATH">Name</th>
              <th className="ATH">Email</th>
              <th className="BTH">Update Password</th>
              <th className="ATH">Status</th>
              <th className="ATH">Delete_User</th>
            </tr>
          </thead>
          <tbody>
            {userData.map((user) => (
              <tr key={user.id}>
                <td>{user.id}</td>
                <td>{user.name}</td>
                <td>{user.email}</td>
                <td style={{ textAlign: "center" }}>
              
              <Popup
                    trigger={<button>Update</button>}
                    position="right center"
                    modal
                    className="custom-popup"
                  >
                    {(close) => (
                      <div className="popup-contents">
                        <div
                          style={{ justifyContent: "right", alignItems: "end" }}
                        >
                          <RiCloseCircleFill onClick={close} size={28} />
                        </div>
                        <h2>Update Password</h2>

                        <div className='input-group'>
                            <input type={showPassword ? 'text' : 'password'}
                                placeholder='Enter password'
                                name='password'
                                className='form-control rounded-0'
                                value={newPassword}
                                onChange={(e) => setNewPassword(e.target.value)}
                                
                            />
                            <button type='button' className='btn btn-outline-secondary' onClick={togglePasswordVisibility}>
                                <FontAwesomeIcon icon={showPassword ? faEyeSlash : faEye} />
                            </button>
                            </div>
                        {/* <input
                          type="password"
                          placeholder="New Password"
                          value={newPassword}
                          onChange={(e) => setNewPassword(e.target.value)}
                        /> */}
                        <div
                          style={{
                            display: "flex",
                            justifyContent: "center",
                            alignItems: "center",
                            marginTop: "10px",
                          }}
                        >
                          <button
                            type="button"
                            className="btn btn-secondary"
                            onClick={() => handlePasswordUpdate(user.id)}
                          >
                            Update
                          </button>
                        </div>
                      </div>
                    )}
                  </Popup>

                  {/* <button type="button" class="btn btn-outline-dark">
                    Update Password 
                  </button> */}
                </td>
                <td
                  style={{
                    backgroundColor:
                      user.status === "Online" ? "#7FFF00" : "#FF0000",
                  }}
                >
                  {user.status}
                </td>
                <td style={{ textAlign: "center" }}>
                  <button onClick={() => deleteUser(user.id)}>Delete</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <input type="text" value={emailInput} readOnly />
    </div>
  );
}

export default Admin;
