import React, { useEffect, useState } from 'react';
import { Line } from 'react-chartjs-2';
import axios from 'axios';
import { useSelector } from 'react-redux';
import Navbar from '../Navbar/Navbar';
import 'chartjs-plugin-zoom';
import { BsFastForwardCircleFill, BsRewindCircleFill } from "react-icons/bs";
import "./Temperature_work.css";

function Minutely() {
    const [chartData, setChartData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [progressPercentage, setProgressPercentage] = useState(0);
    const [error, setError] = useState(null);
    const [startIndex, setStartIndex] = useState(0);
    const [endIndex, setEndIndex] = useState(10);
    const [colors, setColors] = useState([]);

    const dateValue = useSelector((state) => state.dateManager.value);

    useEffect(() => {
        const token = localStorage.getItem('token');
    
        const fetchMinutelyData = async () => {
            setLoading(true);
            setError(null);
    
            try {
                const response = await axios.get(`/api/fetchMinutelyData?selectedDate=${dateValue}`, {
                    headers: { Authorization: `Bearer ${token}` },
                });
                console.log('Fetched minutely data for the selected date:', response.data);
                setChartData(response.data);
                setLoading(false);
            } catch (err) {
                setError('Error fetching minutely data. Please try again.');
                setLoading(false);
            }
        };
    
        fetchMinutelyData();
    
        const intervalId = setInterval(fetchMinutelyData, 60000);
    
        return () => {
            clearInterval(intervalId);
        };
    }, [dateValue]);

    useEffect(() => {
        if (!loading) {
            setColors(generateRandomColors());
        }
    }, [loading]);

    useEffect(() => {
        if (loading) {
            const intervalId = setInterval(() => {
                setProgressPercentage(prevPercentage => {
                    if (prevPercentage < 100) {
                        return prevPercentage + 5;
                    } else {
                        clearInterval(intervalId);
                        return prevPercentage;
                    }
                });
            }, 1000);
            return () => clearInterval(intervalId);
        } else {
            setProgressPercentage(0);
        }
    }, [loading]);

    const handleForward = () => {
        setStartIndex(prevIndex => prevIndex + 10);
        setEndIndex(prevIndex => prevIndex + 10);
    };

    const handleReverse = () => {
        setStartIndex(prevIndex => Math.max(prevIndex - 10, 0));
        setEndIndex(prevIndex => Math.max(prevIndex - 10, 10));
    };

    const generateTimeLabels = () => {
        if (!chartData || chartData.length === 0) {
            return [];
        }
    
        return chartData.slice(startIndex, endIndex).map((entry) => entry.MinutelyDateTime);
    };
    
    const generateDatasets = () => {
        if (!chartData || chartData.length === 0) {
            return [];
        }

        const datasets = [];

        for (let i = 1; i <= 7; i++) {
            const label = `TT${i}`;
            const data = chartData.slice(startIndex, endIndex).map((entry) => entry[label]);

            datasets.push({
                label: label,
                data: data,
                borderColor: colors[i - 1] || '#000',
                fill: false,
            });
        }

        return datasets;
    };

    const generateRandomColors = () => {
        const colors = [];
        for (let i = 0; i < 7; i++) {
            colors.push('#' + Math.floor(Math.random() * 16777215).toString(16));
        }
        return colors;
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
                                    {progressPercentage !== 0 && `${progressPercentage}%`}
                                </div>
                            </div>
                        )}
                        {error && <p style={{ color: 'red' }}>{error}</p>}

                        <div>
                            <div>
                                <button onClick={handleReverse}><BsRewindCircleFill className='BsRewindCircleFill' size={30}/></button>
                                <button onClick={handleForward}><BsFastForwardCircleFill className='BsFastForwardCircleFill' size={30}/></button>
                            </div>
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
                                            text: 'TT1 toTT8 Parameters Minute Wise Data',
                                            font: { size: 25, color: 'rgba(0, 0, 0, 1)' },
                                        },
                                        zoom: {
                                            pan: {
                                                enabled: true,
                                                mode: 'x',
                                            },
                                            zoom: {
                                                pinch: {
                                                    enabled: false,
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

export default Minutely;
