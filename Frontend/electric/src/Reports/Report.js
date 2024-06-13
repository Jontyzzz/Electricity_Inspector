import axios from 'axios';
import React, { useState, useEffect } from 'react';
import Navbar from '../Navbar/Navbar';
import Chart from '../Charts/Chart';
import { useSelector } from 'react-redux';
import { getDataset, getOptionsets } from '../Utility/utilites';
import LoadingSpinner from '../Spinners/Spinner';
import { Navigate } from 'react-router-dom';

function Report() {
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

  const customBarColors = ["rgba(255, 0, 0, 0.7)"];
  const customBarColorss = ["rgba(255,255,0,0.7)"];
  const customBarColorsss = ["rgba(255,0,255,0.7)"];
  const customBarColorsssss = ["rgba(0,0,0,0.7)"];
  const customBarColorssssss = ["rgba(0,0,255)"];
  const customBarColorssss = ["rgba(255,136,0)"];


  const getRunningHour = () => data.find(obj => obj.ParameterName === "Running_Hour")?.ParameterValue || 0;
  const getRunningMinute = () => data.find(obj => obj.ParameterName === "Running_Minute")?.ParameterValue || 0;

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
    <div className='Backenddd'>
      <Navbar />
      <div class="container">
        <div id="box1" class="box">
          <div class="roz">
            <Chart
              chartType="bar"
              data={getDataset(['PT5'],data, "Kg/cm2", 'bar', customBarColorssss)}
              options={getOptionsets(" PARAMETERS OF PT5", 250, 'x')}
            />
          </div>
          <hr />
          <div class="roz">
            <Chart
              chartType="bar"
              data={getDataset(['SFM_2'], data, "Kg/hr", 'bar', customBarColors)}
              options={getOptionsets('PARAMETERS OF SFM_2', 9999)}
            />
          </div>
          <hr />
          <div class="roz">
            <Chart
              chartType="bar"
              data={getDataset(['WFM'], data, "Kg/hr", 'bar', customBarColorsssss)}
              options={getOptionsets(" PARAMETERS OF WFM ", 9999)}
            />
          </div>
          <hr />
          <div class="roz">
            <Chart
              chartType="bar"
              data={getDataset(['TT8'], data, "Deg.C", 'bar', customBarColorsss)}
              options={getOptionsets(" PARAMETERS OF TT8 ", 500)}
            />
          </div>
          <hr />
          {/* <div class="roz">
            <Chart
              chartType="gauge"
              data={getDataset('TT8', Data, "Deg.C", 'gauge')}
              options={getOptionsets(" PARAMETERS OF TT8 ", 500, 'y')}
            />
          </div> */}
          <hr />

          {/* <!---------------------------------- verticle div end --> */}

        </div>
        <div id="box2" class="box">
          <div class="roz">

            <div class="and">
              <div class="roz">
                <Chart
                  chartType="pie"
                  data={getDataset(['Boiler_Steam_Quality_Dryness'], data, "%", 'pie', customBarColorss)}
                  options={getOptionsets(" PARAMETERS OF Boiler Steam Quality Dryness ", 100)}
                />
              </div>
            </div>
            <hr />
            <div className='roz'>
              <Chart
                chartType="bar"
                data={getDataset(['Totalizer_SFM_2'], data, "Ton", 'bar', customBarColorssssss)}
                options={getOptionsets(" PARAMETERS of Totalizer SFM_2 ", 99999999)}
              />
            </div>
          </div>
          <hr />
          {data && data.length > 0 && <div class="roz">
            {/* <div className='Runn'> */}
            <div className='Runn1'>
              <p><b> RunningHour </b> : <input
                type="text"
                value={getRunningHour()}
                readOnly
              /></p>
            </div>
            <br />
            <div className='Runn2'>
              <p><b>RunningTime</b>: <input
                type="text"
                value={getRunningMinute()}
                readOnly
              /></p>
            </div>
            {/* </div> */}
          </div>}
          <hr />
          <div className='roz'>
            <Chart
              chartType="line"
              data={getDataset(['TT1', 'TT2', 'TT3', 'TT4', 'TT5', 'TT6', 'TT7'], data, 'Deg.C', 'line', customBarColorss)}
              options={getOptionsets('PARAMETERS OF TT1 to TT7', 500)}
            />
          </div>
        </div>

        {/* <!---------------------------------- verticle div end --> */}


      </div>
      {/* Actually what happened is temperature chart component is having navbar too me reflected */}
      {/* <div className="col-md-8 mb-4 mt-4 bg-white"> */}
      <div className='container'>
       {/* <TemperatureLineChart/> */}
      </div>



    </div>
  )
}

export default Report;
