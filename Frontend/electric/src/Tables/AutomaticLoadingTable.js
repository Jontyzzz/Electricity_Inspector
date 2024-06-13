import axios from 'axios';
import React, { useState, useEffect } from 'react';
import Navbar from '../Navbar/Navbar';
import Parameter_unit from '../Parameter_unit.json';
import { io } from 'socket.io-client';
import { useSelector } from 'react-redux';
import { webSocketUrl } from '../Utility/localstorage';
import LoadingSpinner from '../Spinners/Spinner';
import { Navigate } from 'react-router-dom';
import jsPDF from 'jspdf';
import 'jspdf-autotable';



function AutomaticLoadingTable() {
  const [Data, setData] = useState([]);
  const dateValue = useSelector(state => state.dateManager.value);
  const [isLoading, setIsLoading] = useState(1)


  useEffect(() => {
    const token = localStorage.getItem("token");

    // Fetch initial data
    axios.get(`/api/getdata?date=${dateValue}`, {
      headers: { Authorization: `Bearer ${token}` }
    })
      .then((res) => {
        setData(res.data);
        setIsLoading(2);
      })
      .catch(err => {
        console.log(err)
        setIsLoading(3);
      });

    // Establish a WebSocket connection
    const ws = new WebSocket(webSocketUrl);

    // Set up event listeners
    ws.addEventListener('open', () => console.log('WebSocket connection opened'));
    ws.addEventListener('message', (event) => {
      console.log(`Received message: ${event.data}`)
      const token = localStorage.getItem("token");
      axios.get(`/api/getdata?date=${dateValue}`, {
        headers: { Authorization: `Bearer ${token}` }
      })
        .then((res) => {
          setData(res.data);
          setIsLoading(2);
        })
        .catch(err => {
          console.log(err)
          setIsLoading(3);
        });

    });
    ws.addEventListener('close', () => console.log('WebSocket connection closed'));

    // Clean up the WebSocket connection on component unmount
    return () => ws.close();

  }, [dateValue]); // Empty dependency array means this effect runs once on mount
  const downloadPDF = () => {
    const doc = new jsPDF();

    // Add title
    doc.setFontSize(18);
    doc.text("Spectron_ONGC Report", 105, 20, { align: "center" });

    // Add date/time
    doc.setFontSize(12);
    doc.text(new Date().toLocaleString(), 10, 10);

    // Add table headers
    const headers = [['Machine PID', 'Parameter Name', 'Parameter Value', 'Unit']];

    // Map Data array to match the expected structure for autoTable
    const tableData = Data.map(row => [row.machinePID, row.parameterName, row.parameterValue, row.unit]);

    // Combine headers and table data
    const allData = [...headers, ...tableData];

    // Add table using autoTable
    doc.autoTable({
        startY: 30, // Initial y position for table
        head: allData.slice(0, 1), // Extract headers
        body: allData.slice(1), // Extract table data excluding headers
        theme: 'grid', // Optional: set table theme
        styles: { overflow: 'linebreak' } // Optional: handle long text
    });

    // Save the PDF
    doc.save('data.pdf');
};

  if (isLoading === 1) {
    return (<>
      <Navbar />
      <LoadingSpinner />
    </>)
  }
  if (isLoading === 3) {
    return (
      <Navigate to="/" />
    )
  }

  return (
    <div className='backend'>
      <Navbar />
      <div className='center2'>
        <div className='col-15 color ' alignment="center"></div>
        <button style={{ backgroundColor: 'red', color: 'white' }}onClick={downloadPDF}>Download PDF</button>
        <div class="tbl col-15 " alignment="center">
          <table alignment="center">
            <tr class='header'>
              <th ALIGN='CENTER'>Machine PID</th>
              <th align='right'>Parameter Name</th>
              <th>Parameter Value</th>
              <th>Unit</th>
            </tr>
            <tbody>
              {Data.map((innerdata) => (
                <tr key={innerdata.PID}>
                  <td align='center'>{innerdata.PID}</td>
                  <td>{innerdata.ParameterName}</td>
                  <td align='right'>{innerdata.ParameterValue}</td>
                  <td>{Parameter_unit[innerdata.ParameterName]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className='col-3 color '></div>
      </div>
      <div class="card bg-warning-subtle">
        <div class="card-body">
          <i className='timt'>copyright &#169; Protovec Technology Pvt Ltd. All rights reserved 2023</i>
        </div>
      </div>
    </div>
  );
}

export default AutomaticLoadingTable;
