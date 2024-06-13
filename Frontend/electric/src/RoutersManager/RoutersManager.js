

import React, { useState, useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Report from '../Reports/Report';
import AutomaticLoadingTable from '../Tables/AutomaticLoadingTable';
import LiveReports from '../Reports/LiveReports';
import Login from '../Login/login';
import Signup from '../Login/signup';
import StaticTable from '../Tables/StaticTable';
// import TemperatureLineChart from '../Charts/TemperatureLineChart';
import ErrorPage from '../ErrorsPage/ErrorsPage';
import Admin from '../Login/Admin';
import Temperature_works from '../Charts/Temperature_Works';
import Employee from '../Login/Employee';
import Demo from '../Login/Demo';
import Hourly from '../Charts/Hourly';
import Minutely from '../Charts/Minutely';
import Navbar2 from '../Navbar/Navbar2';


function RoutersManager() {
  const [isAdmin, setIsAdmin] = useState(false);

  // Function to decode the token
  const decodeToken = (token) => {
    try {
      const base64Url = token.split('.')[1];
      const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
      const jsonPayload = decodeURIComponent(atob(base64).split('').map((c) => {
        return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
      }).join(''));
      return JSON.parse(jsonPayload);
    } catch (error) {
      console.error('Error decoding token:', error);
      return null;
    }
  };

  useEffect(() => {
    const token = localStorage.getItem('token');
    if (token) {
      const decodedToken = decodeToken(token);
      setIsAdmin(decodedToken.role === 'admin');
    }
  }, []);

  return (
    <div>
      <BrowserRouter>
        <Routes>
          <Route path='/' element={<Login />} />
          <Route path='/signup' element={<Signup />} />
          <Route path='/AutomaticLoadingTable' element={<AutomaticLoadingTable />} />
          <Route path='/Report' element={<Report />} />
          <Route path='/LiveReports' element={<LiveReports />} />
          <Route path='/StaticTable' element={<StaticTable />} />
          {/* <Route path='/Admin' element={<Admin />} /> */}
          {isAdmin ? <Route path='/Admin' element={<Admin />} /> : <Route path='/Admin' element={<ErrorPage />} />}
          <Route path='/Temperature_Works' element={<Temperature_works />} />
          <Route path='/ErrorsPage' element={<ErrorPage />} />
          <Route path='/Employee' element={<Employee />} />
          <Route path='/Demo' element={<Demo />} />
          <Route path='/Hourly' element={<Hourly />} />
          <Route path='/Minutely' element={<Minutely />} />
          <Route path='/Navbar2' element={<Navbar2 />} />
        </Routes>
      </BrowserRouter>
    </div>
  );
}

export default RoutersManager;
