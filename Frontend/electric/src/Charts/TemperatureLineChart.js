import React, { useEffect, useState } from 'react';
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler } from 'chart.js';
import { Line } from 'react-chartjs-2';
import axios from 'axios';
import io from 'socket.io-client';
import Navbar from '../Navbar/Navbar';
import { useSelector } from 'react-redux';
import LoadingSpinner from '../Spinners/Spinner';
import { Navigate } from 'react-router-dom';


ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler);

function TemperatureLineChart() {
    const [chartData, setChartData] = useState([]);
    const [socket, setSocket] = useState(null);
    const [timeGranularity, setTimeGranularity] = useState('hour');
    const [isLoading, setIsLoading] = useState(1);
    const [error, setError] = useState(null);
    const dateValue = useSelector(state => state.dateManager.value);

    useEffect(() => {
        console.log(dateValue);
        const token = localStorage.getItem("token");

        // const fetchData = () => {
            axios.get(`/api/fetchData?date=` + dateValue, {
                headers: { Authorization: `Bearer ${token}` }
            })
                .then((res) => {
                    console.log('Data received from server:', res.data);
                    setChartData(res.data);
                    setIsLoading(2);
                })
                .catch(err => {
                    console.error('Error fetching data:', err);
                    setError('Error fetching data. Please try again.');
                    setIsLoading(3);
                });
        // };

        const socket = io('http://localhost:8011/socket.io');

        socket.on('connect', () => {
            console.log('Socket.IO connection opened');
            // fetchData();
        });

        socket.on('dataUpdate', (data) => {
            console.log('Real-time data received:', data);
            // setChartData(data);
            axios.get(`/api/fetchData?date=` + dateValue, {
                headers: { Authorization: `Bearer ${token}` }
            })
                .then((res) => {
                    console.log('Data received from server:', res.data);
                    setChartData(res.data);
                    
                })
                .catch(err => {
                    console.error('Error fetching data:', err);
                    setError('Error fetching data. Please try again.');
                    setIsLoading(3);
                });

        });

        socket.on('disconnect', () => {
            console.log('Socket.IO connection closed');
        });

        setSocket(socket);

        return () => {
            socket.disconnect();
        };
    }, [dateValue]);
    const generateDatasets = () => {
        if (!chartData || chartData.length === 0) {
            return [];
        }

        const datasets = [];

        for (let i = 1; i <= 7; i++) {
            const label = `TT${i}`;
            const data = chartData.map(entry => entry[label]);
            const color = getRandomColor();

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

        const timeLabels = chartData.map((entry) => {
            const date = new Date(entry.ConDate);
            return isNaN(date.getTime()) ? '' : timeGranularity === 'hour' ? `${date.getHours()}:00` : `${date.getHours()}:${date.getMinutes()}`;
        });

        return Array.from(new Set(timeLabels));
    };

    const getRandomColor = () => {
        return '#' + Math.floor(Math.random() * 16777215).toString(16);
    };

    const handleToggleGranularity = () => {
        setTimeGranularity(prevGranularity => (prevGranularity === 'hour' ? 'minute' : 'hour'));
    };

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
        <div className='Temp_Background'>
            <Navbar />
            <React.Fragment>
                <div className="container-fluid">
                    <div>
                        <div className="col-md-10 mb-4 mt-4 ml-4 Tempp" style={{ overflowX: 'scroll', height: '600px' }}>
                            {isLoading && <p>Loading...</p>}
                            {error && <p style={{ color: 'red' }}>{error}</p>}
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
                                    },
                                    scales: {
                                        x: {
                                            position: 'bottom',
                                            grid: {
                                                display: true,
                                            },
                                            title: {
                                                display: true,
                                                text: 'Data According to Time',
                                                font: { size: 20, color: 'rgba(0, 0, 0, 1)' },
                                            },
                                        },
                                        y: {
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
                            <button onClick={handleToggleGranularity} className="buttonDecorative" >Minutes OR Hours</button>
                        </div>
                    </div>
                </div>
            </React.Fragment>
        </div>
    );
}

export default TemperatureLineChart;
