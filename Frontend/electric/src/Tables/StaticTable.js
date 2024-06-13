import axios from 'axios';
import React, { useState, useEffect } from 'react';
import Navbar from '../Navbar/Navbar';
import { useSelector } from 'react-redux';
import LoadingSpinner from '../Spinners/Spinner';
import { Navigate } from 'react-router-dom';
import 'jspdf-autotable';

function StaticTable() {
    const [Data, setData] = useState([]);
    const dateValue = useSelector(state => state.dateManager.value);
    const [isLoading, setIsLoading] = useState(1);
    const [lastUpdated, setLastUpdated] = useState(null);

    useEffect(() => {
        console.log(dateValue);
        const token = localStorage.getItem("token");
         
    
        // Function to fetch data
        const fetchData = () => {
            console.log('Fetching data...'); // Log each time data is fetched
            axios.get('/api/getdata?date=' + dateValue, {
                headers: { Authorization: `Bearer ${token}` }
            })
                .then((res) => {
                    setData(res.data);
                    setLastUpdated(new Date().toLocaleTimeString()); // Update timestamp
                    setIsLoading(false);
                    console.log('Fetched data:', res.data); // Log fetched data to console
                })
                .catch(err => {
                    console.log(err)
                    setIsLoading(false);
                });
        };
    
        // Fetch initial data
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
    
    
    // const downloadExcel = () => {
    //     const ws = XLSX.utils.json_to_sheet(Data);
    //     const wb = XLSX.utils.book_new();
    //     XLSX.utils.book_append_sheet(wb, ws, "Data");
    //     XLSX.writeFile(wb, "data.xlsx");
    // };
    // const downloadPDF = () => {
    //      doc = new jsPDF();
    
    //     // Add title
    //     doc.seconsttFontSize(18);
    //     doc.text("Spectron_ONGC Report", 105, 20, { align: "center" });
    
    //     // Add date/time
    //     doc.setFontSize(12);
    //     doc.text(new Date().toLocaleString(), 10, 10);
    
    //     // Add table data
    //     let y = 30; // Initial y position for table data
    //     Data.forEach(row => {
    //         Object.values(row).forEach((value, index) => {
    //             doc.text(value.toString(), index * 50, y);
    //         });
    //         y += 10; // Increment y position for each row
    //     });
    
    //     // Save the PDF
    //     doc.save('data.pdf');
    // };
   
    

    if (isLoading === 1) {
        return (
            <>
                <Navbar />
                <LoadingSpinner />
            </>
        );
    }

    if (isLoading === 3) {
        return (
            <>
                <Navbar />
                <Navigate to="/" />
            </>
        );
    }


    return (
        <div className='park'>
            <Navbar />

            <div class="tbl col-8 bark " alignment="center" >
            {/* <button style={{ backgroundColor: 'green', color: 'white' }} onClick={downloadExcel}>Download Report</button> */}
            {/* <p>Last Updated: {lastUpdated}</p> */}
                <table alignment="center"id='Static-Table' className='tablet'>
                <thead>
                    <tr class='header'>
                        <th className='stth' ALIGN='CENTER'>Machine PID</th>
                        <th className='stth' align='right'>Parameter Name</th>
                        <th className='stth' >Parameter Value</th>
                        <th className='stth' >Unit</th>
                    </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>1</td>
                            <td>PT1</td>
                            <td>
                                {Data.map((innerdata, index) => (
                                    index === 0 && (
                                        <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                            <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                        </React.Fragment>
                                    )
                                ))}
                            </td>
                            <td>Kg/cm2</td>
                        </tr>
                        <tr>
                            <td>2</td>
                            <td>PT2</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 1 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Kg/cm2</td>
                        </tr>
                        <tr>
                            <td>3</td>
                            <td>PT3</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 2 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Kg/cm2</td>
                        </tr>
                        <tr>
                            <td>4</td>
                            <td>PT4</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 3 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Kg/cm2</td>
                        </tr>
                        <tr>
                            <td>5</td>
                            <td>PT5</td>
                            <td >{Data.map((innerdata, index) => (
                                index === 4 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class=' TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Kg/cm2</td>
                        </tr>
                        <tr>
                            <td>6</td>
                            <td>CM1</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 5 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>PPM</td>
                        </tr>
                        <tr>
                            <td>7</td>
                            <td>CM2</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 6 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>PPM</td>
                        </tr>
                        <tr>
                            <td>8</td>
                            <td>GFM</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 7 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Kg/hr</td>
                        </tr>
                        <tr>
                            <td>9</td>
                            <td>OFM</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 8 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Kg/hr</td>
                        </tr>
                        <tr>
                            <td>10</td>
                            <td>SFM_1</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 9 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Kg/hr</td>
                        </tr>
                        <tr>
                            <td>11</td>
                            <td>SFM_2</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 10 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Kg/hr</td>
                        </tr>
                        <tr>
                            <td>12</td>
                            <td>WFM</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 11 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Kg/hr</td>
                        </tr>
                        <tr>
                            <td>13</td>
                            <td>DI</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 12 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>mmwc</td>
                        </tr>
                        <tr>
                            <td>14</td>
                            <td>TT1</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 13 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Deg.C</td>
                        </tr>
                        <tr>
                            <td>15</td>
                            <td>TT2</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 14 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Deg.C</td>
                        </tr>
                        <tr>
                            <td>16</td>
                            <td>TT3</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 15 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Deg.C</td>
                        </tr>
                        <tr>
                            <td>17</td>
                            <td>TT4</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 16 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Deg.C</td>
                        </tr>
                        <tr>
                            <td>18</td>
                            <td>TT5</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 17 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Deg.C</td>
                        </tr>
                        <tr>
                            <td>19</td>
                            <td>TT6</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 18 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Deg.C</td>
                        </tr>
                        <tr>
                            <td>20</td>
                            <td>TT7</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 19 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Deg.C</td>
                        </tr>
                        <tr>
                            <td>21</td>
                            <td>TT8</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 20 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Deg.C</td>
                        </tr>
                        <tr>
                            <td>22</td>
                            <td>Boiler Steam Quality (Dryness)</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 21 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>%</td>
                        </tr>
                        <tr>
                            <td>23</td>
                            <td>Totalizer GFM</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 22 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Ton</td>
                        </tr>
                        <tr>
                            <td>24</td>
                            <td>Totalizer OFM</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 23 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Ton</td>
                        </tr>
                        <tr>
                            <td>25</td>
                            <td>Totalizer SFM_1</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 24 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Ton</td>
                        </tr>
                        <tr>
                            <td>26</td>
                            <td>Totalizer SFM_2</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 25 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Ton</td>
                        </tr>
                        <tr>
                            <td>27</td>
                            <td>Totalizer WFM</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 26 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>Ton</td>
                        </tr>
                        <tr>
                            <td>28</td>
                            <td>TWCVFB_S</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 27 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>%</td>
                        </tr>
                        <tr>
                            <td>29</td>
                            <td>TWCVFB_W</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 28 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>%</td>
                        </tr>
                        <tr>
                            <td>30</td>
                            <td>Running Hour</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 29 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>HR</td>
                        </tr>
                        <tr>
                            <td>31</td>
                            <td>Running Minute</td>
                            <td>{Data.map((innerdata, index) => (
                                index === 30 && (
                                    <React.Fragment key={innerdata.PID} class='TDCSStr'>
                                        <td align='right' class='TDCSS'>{innerdata.ParameterValue}</td>
                                    </React.Fragment>
                                )
                            ))}</td>
                            <td>MIN</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    )
}

export default StaticTable
