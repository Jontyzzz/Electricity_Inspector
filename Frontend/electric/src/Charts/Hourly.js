import React, { useEffect, useState } from 'react';
import { Line } from 'react-chartjs-2';
import axios from 'axios';
import { useSelector } from 'react-redux';
import Navbar from '../Navbar/Navbar';
import 'chartjs-plugin-zoom';
import "./Temperature_work.css";

// Function to generate random color
const getRandomColor = () => {
    return '#' + Math.floor(Math.random() * 16777215).toString(16);
};

function Hourly() {
    const [chartData, setChartData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [progressPercentage, setProgressPercentage] = useState(0);
    const [error, setError] = useState(null);
    const dateValue = useSelector((state) => state.dateManager.value);

    useEffect(() => {
        const token = localStorage.getItem('token');
    
        const fetchData = async () => {
            setLoading(true);
            setError(null);
    
            try {
                const response = await axios.get(`/api/fetchData?selectedDate=${dateValue}`, {
                    headers: { Authorization: `Bearer ${token}` },
                });
                console.log('Fetched hourly data for the selected date:', response.data);
                setChartData(response.data);
                setLoading(false);
            } catch (err) {
                setError('Error fetching hourly data. Please try again.');
                setLoading(false);
            }
        };
    
        fetchData(); // Fetch initial hourly data
    
        const intervalId = setInterval(fetchData, 60000); // Fetch hourly data every 1 minute
    
        return () => {
            clearInterval(intervalId); // Cleanup interval on component unmount
        };
    }, [dateValue]);
    

    useEffect(() => {
        // Update progress percentage based on loading state
        if (loading) {
            const intervalId = setInterval(() => {
                setProgressPercentage(prevPercentage => {
                    if (prevPercentage < 100) {
                        return prevPercentage + 5; // Increment progress by 5% every second until it reaches 100%
                    } else {
                        clearInterval(intervalId); // Stop interval when progress reaches 100%
                        return prevPercentage;
                    }
                });
            }, 1000);
            return () => clearInterval(intervalId); // Cleanup interval on component unmount or when loading is done
        } else {
            setProgressPercentage(0); // Reset progress percentage when loading is complete
        }
    }, [loading]);

    const generateTimeLabels = () => {
        if (!chartData || chartData.length === 0) {
            return [];
        }
    
        return chartData.map((entry) => entry.HourlyDateTime);
    };
    
    const generateDatasets = () => {
        if (!chartData || chartData.length === 0) {
            return [];
        }

        const datasets = [];

        for (let i = 1; i <= 7; i++) {
            const label = `TT${i}`;
            const data = chartData.map((entry) => entry[label]);

            datasets.push({
                label: label,
                data: data,
                borderColor: getRandomColor(), // Add color if needed
                fill: false, // Ensure no fill for lines
            });
        }

        return datasets;
    };

    return (
        <div className='BGTemp'>
            <Navbar />
            <div className="container-fluid ">
                <div>
                    <div className="col-md-10 mb-4 mt-4 ml-4 Tempp" style={{ overflowX: 'auto' }}>
                        {loading && (
                            <div className="line-progress-bar">
                                <div className="line-progress-bar-fill" style={{ width: `${progressPercentage}%` }}>
                                    {progressPercentage !== 0 && `${progressPercentage}%`} {/* Display progress percentage if not zero */}
                                </div>
                            </div>
                        )}
                        {error && <p style={{ color: 'red' }}>{error}</p>}

                        <div>
                            <Line
                                data={{
                                    labels: generateTimeLabels(),
                                    datasets: generateDatasets(),
                                }}
                                options={{
                                    responsive: true,
                                    plugins: {
                                        legend: {},
                                        title: {
                                            display: true,
                                            text: 'TT1 toTT8 Parameters Hour Wise Data',
                                            font: { size: 25, color: 'rgba(0, 0, 0, 1)' },
                                        },
                                        zoom: {
                                            pan: {
                                                enabled: true,
                                                mode: 'x',
                                            },
                                            zoom: {
                                                pinch: {
                                                    enabled: false, // Disable pinch zoom
                                                },
                                                wheel: {
                                                    enabled: true,
                                                },
                                                mode: 'x',
                                                sensitivity: 0.5,
                                            },
                                        },
                                    },
                                    scales: {
                                        x: {
                                            position: 'bottom',
                                            grid: {
                                                display: true,
                                            },
                                            title: {
                                                display: true,
                                                text: 'Data According to Time ',
                                                font: { size: 20, color: 'rgba(0, 0, 0, 1)' },
                                            },
                                        },
                                        y: {
                                            beginAtZero: true,
                                            grid: {
                                                display: true,
                                            },
                                            title: {
                                                display: true,
                                                text: 'Data in Range',
                                                font: { size: 20, color: 'rgba(0, 0, 0, 1)' },
                                            },
                                        },
                                    },
                                }}
                            />
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default Hourly;
