import axios from 'axios';
import React, { useState, useEffect } from 'react';
import Navbar from '../Navbar/Navbar';
import Chart from '../Charts/Chart';
import { useSelector } from 'react-redux';
import { getDataset, getOptionsets } from '../Utility/utilites';
import LoadingSpinner from '../Spinners/Spinner';
import { Navigate } from 'react-router-dom';

function LiveReports() {
  const [data, setData] = useState([]);
  const dateValue = useSelector(state => state.dateManager.value);
  const [isLoading, setIsLoading] = useState(1);

  // Function to fetch data
  const fetchData = async () => {
    try {
      const token = localStorage.getItem("token");
      const response = await axios.get(`/api/getdata?date=${dateValue}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setData(response.data);
      setIsLoading(2);
      console.log('Fetched data:',response.data); 
    } catch (err) {
      console.log(err);
      setIsLoading(3);
    }
  };

  useEffect(() => {
    // Fetch data initially
    fetchData();

    // Check if the selected date is today's date
    const isToday = dateValue === new Date().toISOString().slice(0, 10);

    // Start setInterval only if it's today's date
    if (isToday) {
      // Set interval to fetch data every 1 minute
      const interval = setInterval(() => {
        fetchData();
      }, 60000);

      // Clear interval on component unmount
      return () => clearInterval(interval);
    }
  }, [dateValue]);


  const customBarColor = ["red"];
  const customBarColorr = ["rgba(255,0,255,0.6)"];

  if (isLoading === 1) {
    return (<>
      <Navbar />
      <LoadingSpinner />
    </>)
  }
  if (isLoading === 3) {
    return (
      <Navigate to="/ErrorPage" />
    )
  }

  return (
    <div className='Backenddd'>
      <Navbar />
      <div className="container">
        <div id="box1" className="box">
          <div className="roz">
            <Chart
              chartType="bar"
              data={getDataset(['PT1', 'PT2', 'PT3', 'PT4'], data, 'Kg/cm2')}
              options={getOptionsets(" PARAMETERS PT1 To PT5", 250, 'x')}
            />
          </div>
          <hr />
          <div className="roz">
            <Chart
              chartType="bar"
              data={getDataset(['CM1', 'CM2'], data, "PPM")}
              options={getOptionsets(" PARAMETERS OF CM1 & CM2 ", 2000, 'x')}
            />
          </div>
          <hr />
          <div className="roz">
            <Chart
              chartType="bar"
              data={getDataset(['Totalizer_SFM_1', 'Totalizer_GFM', 'Totalizer_OFM'], data, "Ton", 'bar')}
              options={getOptionsets("PARAMETERS OF Totalizer_GFM & Totalizer_SFM_1 Totalizer_OFM", 99999999, 'x')}
            />
          </div>
          <hr />
          <div className="roz">
            <Chart
              chartType="bar"
              data={getDataset(['OFM'], data, "Kg/hr")}
              options={getOptionsets(" PARAMETERS  OFM ", 1890)}
            />
          </div>
          <hr />
          <div className="roz">
            <Chart
              chartType="bar"
              data={getDataset(['SFM_1'], data, "Kg/hr", 'bar', customBarColorr)}
              options={getOptionsets(" PARAMETERS  SFM_1 ", 9999, 'y')}
            />
          </div>
        </div>
        <div id="box2" className="box">
          <div className="roz">
            <Chart
              chartType="bar"
              data={getDataset(['TT1', 'TT2', 'TT3', 'TT4', 'TT5', 'TT6', 'TT7'], data, "Deg.C")}
              options={getOptionsets(" PARAMETERS OF TT1 to TT7", 500, 'x')}
            />
          </div>
          <hr />
          <div className="roz">
            <div className='and'>
              <Chart
                chartType="pie"
                data={getDataset(['TWCVFB_S', 'TWCVFB_W'], data, "%", 'pie')}
                options={getOptionsets("PARAMETERS OF TWCVFB_S & TWCVFB_W", 100)}
              />
            </div>
          </div>
          <hr />
          <div className="roz">
            <Chart
              chartType="bar"
              data={getDataset(['DI'], data, "mmvc", 'bar', customBarColor)}
              options={getOptionsets("PARAMETERS OF DI", 600)}
            />
          </div>
          <hr />
          <div className="roz">
            <Chart
              chartType="bar"
              data={getDataset(['GFM'], data, "Kg/hr")}
              options={getOptionsets("PARAMETERS OF GFM", 600, 'y')}
            />
          </div>
          <hr />
          <div className="roz">
            <Chart
              chartType="doughnut"
              data={getDataset(['Totalizer_SFM_1', 'Totalizer_GFM', 'Totalizer_OFM'], data, "Ton", 'doughnut')}
              options={getOptionsets("PARAMETERS OF Totalizer_GFM & Totalizer_SFM_1 Totalizer_OFM", 99999999, 'x')}
            />
          </div>
        </div>
      </div>
    </div>
  );
}

export default LiveReports;
