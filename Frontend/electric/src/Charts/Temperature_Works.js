import React, { useEffect, useState } from 'react';
import { Line } from 'react-chartjs-2';
import axios from 'axios';
import { useSelector } from 'react-redux';
import Navbar from '../Navbar/Navbar';
import 'chartjs-plugin-zoom';
import { format, parseISO } from 'date-fns';
import "./Temperature_work.css";
import { useNavigate } from 'react-router-dom';

function Temperature_works() {
    const [chartData, setChartData] = useState([]);
    const [timeGranularity, setTimeGranularity] = useState('hour');
    const [loading, setLoading] = useState(true);
    const [progressPercentage, setProgressPercentage] = useState(0);
    const [error, setError] = useState(null);
    const dateValue = useSelector((state) => state.dateManager.value);
    const [visibleRange, setVisibleRange] = useState({ min: 0, max: 11 });
    const [lineColors, setLineColors] = useState({});
    const navigate = useNavigate();



    //     fetchData(); // Fetch initial data

    //     const interval = setInterval(fetchData, 3000); // Fetch data every 1 minute

    //     return () => {
    //         clearInterval(interval); // Cleanup interval on component unmount
    //     };
    // }, [dateValue]);
    useEffect(() => {
        const token = localStorage.getItem('token');

        const fetchData = async () => {
            setLoading(true);
            setError(null);

            try {
                const response = await axios.get('/api/fetchData?selectedDate=' + dateValue, {
                    headers: { Authorization: `Bearer ${token}` },
                });
                console.log('Fetched data for the selected date:', response.data);
                const initialLineColors = {};
                for (let i = 1; i <= 7; i++) {
                    const label = `TT${i}`;
                    initialLineColors[label] = getRandomColor();
                }

                setLineColors(initialLineColors);
                setChartData(response.data);
                setLoading(false);
            } catch (err) {
                setError('Error fetching data. Please try again.');
                setLoading(false);
            }
        };

        fetchData(); // Fetch initial data

        const intervalId = setInterval(fetchData, 60000); // Fetch data every 3 seconds

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

    const generateDatasets = () => {
        if (!chartData || chartData.length === 0) {
            return [];
        }

        const datasets = [];

        for (let i = 1; i <= 7; i++) {
            const label = `TT${i}`;
            const data = chartData.slice(visibleRange.min, visibleRange.max + 1).map((entry) => entry[label]);
            const color = lineColors[label];

            datasets.push({
                label: label,
                data: data,
                borderColor: color,
                backgroundColor: color,
                hoverOffset: 3,
            });
        }

        return datasets;
    };

    const generateTimeLabels = () => {
        if (!chartData || chartData.length === 0) {
            return [];
        }

        const timeLabels = chartData.slice(visibleRange.min, visibleRange.max + 1).map((entry) => {
            const dateTimeString = entry.DateTime;

            if (!dateTimeString) {
                return '';
            }

            try {
                const date = parseISO(dateTimeString); // Parse the datetime string
                let formattedTime = '';

                if (timeGranularity === 'hour') {
                    formattedTime = format(date, 'HH:mm'); // Format only hours and minutes
                } else {
                    formattedTime = format(date, 'HH:mm:ss'); // Format hours, minutes, and seconds
                }

                return formattedTime;
            } catch (error) {
                console.error(`Error parsing datetime: ${dateTimeString}`, error);
                return ''; // Handle parsing errors
            }
        });

        return timeLabels;
    };

    const handleScrollUp = () => {
        const { min, max } = visibleRange;
        const dataLength = chartData.length;

        if (max < dataLength - 1) {
            setVisibleRange((prevRange) => ({ min: prevRange.min + 1, max: prevRange.max + 1 }));
        }
    };

    const handleScrollDown = () => {
        const { min, max } = visibleRange;

        if (min > 0) {
            setVisibleRange((prevRange) => ({ min: prevRange.min - 1, max: prevRange.max - 1 }));
        }
    };

    const getRandomColor = () => {
        return '#' + Math.floor(Math.random() * 16777215).toString(16);
    };

    const handleToggleGranularity = () => {
        setTimeGranularity((prevGranularity) =>
            prevGranularity === 'hour' ? 'minute' : 'hour'
        );
    };

    const handleClickHourly = () => {
        // Route to another page
        navigate('/Hourly');
    
    };
    const handleClickMinutely = () => {
        // Route to another page
        navigate('/Minutely');
    
    };

    return (
        <div className='BGTemp'>
            <Navbar />
            <button onClick={handleClickHourly} style={{ backgroundColor: 'red', color: 'black' , marginRight:'3px'}}>Hourly Data Page</button>
            <button onClick={handleClickMinutely} style={{ backgroundColor: 'green', color: 'black', marginLeft:'3px' }}>Minute wise Data Page</button>

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

                        <div style={{ display: 'flex', alignItems: 'center' }}>
                            <button onClick={handleScrollUp} className="buttonDecorative" style={{  backgroundColor: 'grey', color: 'white' }}>
                                {'<<'}
                            </button>
                            
                            <button onClick={handleScrollDown} className="buttonDecorative" style={{ backgroundColor: 'grey', color: 'white' }}>
                                {'>>'}
                            </button>

                        </div>
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
                                            text: `TT1 TO TT7 Parameters - ${timeGranularity.toUpperCase()} wise`,
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
                                            min: visibleRange.min,
                                            max: visibleRange.max,
                                            grid: {
                                                display: true,
                                            },
                                            title: {
                                                display: true,
                                                text: 'Data According to Time',
                                                font: { size: 20, color: 'rgba(0, 0, 0, 1)' },
                                            },
                                            // Enable scrollbar
                                            scrollbar: {
                                                enabled: true,
                                                mode: 'x',
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
                        <button onClick={handleToggleGranularity} className="buttonDecorative">
                            Minutes OR Hours
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default Temperature_works;
