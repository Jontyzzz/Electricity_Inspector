import React, { useState, useEffect } from 'react';
import { BiDownload } from "react-icons/bi";
import axios from 'axios';
import * as XLSX from 'xlsx';
import { useSelector, useDispatch } from 'react-redux';
import { setDateValue } from '../Redux/dateManager';
import { BiSolidUserRectangle } from "react-icons/bi";
import { Navigate, useNavigate } from 'react-router-dom';
import jsPDF from 'jspdf';
import autoTable from 'jspdf-autotable'
import { Link } from 'react-router-dom';





// Function to decode JWT token
const decodeJwtToken = (token) => {
    if (token) {
        try {
            const payloadBase64 = token.split('.')[1];
            const decodedPayload = JSON.parse(atob(payloadBase64));
            return decodedPayload;
        } catch (error) {
            console.error('Error decoding JWT token:', error);
            return null;
        }
    } else {
        console.error('Token is undefined or null');
        return null;
    }
};

function Navbar() {
    const dateValue = useSelector(state => state.dateManager.value);
    const [showDropdown, setShowDropdown] = useState(false);
    const [showDropdown2, setShowDropdown2] = useState(false);
    const [date, setDate] = useState(dateValue);
    const dispatch = useDispatch();
    const navigateFunction = useNavigate();





    useEffect(() => {
        const today = new Date().toISOString().split('T')[0];
        dispatch(setDateValue(today));
    }, [dispatch]);

    useEffect(() => {
        function handleClickOutside(event) {
            const dropdownMenu = document.querySelector('.dropdown-menu');
            const dropdown2Menu = document.querySelector('.dropdown2-menu');

            if (dropdownMenu && !dropdownMenu.contains(event.target)) {
                setShowDropdown(false);
            }
            if (dropdown2Menu && !dropdown2Menu.contains(event.target)) {
                setShowDropdown2(false);
            }

        }

        document.addEventListener("mousedown", handleClickOutside); // Listen for mousedown events
        return () => {
            document.removeEventListener("mousedown", handleClickOutside); // Cleanup on unmount
        };
    }, []);

    const dateChange = (e) => {
        const newDate = e.target.value;
        dispatch(setDateValue(newDate));
        setDate(newDate);
    }

    const handleButtonClick = () => {
        const token = localStorage.getItem('token');
        console.log("entered handle button click function")
        const decodedToken = decodeJwtToken(token);
        console.log('Decoded Token:', decodedToken);
        console.log("entered handle button click function")

        if (!decodedToken || !decodedToken.email) {
            console.error('Invalid token or missing email');
            return;
        }

        axios.get('/api/getUserRole', {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        })
            .then(response => {
                const role = response.data.role;
                console.log("User Role : ", role)
                if (role === 'employee') {
                    navigateFunction('/Employee');
                } else if (role === 'admin') {
                    navigateFunction('/Admin');
                } else {
                    console.error('Unknown or unsupported role:', role);
                }
            })
            .catch(error => {
                console.error('Error fetching user role:', error);
            });
    }

    const handleLogoutButtonClick = () => {
        const token = localStorage.getItem('token');
        console.log("entered handle button click function");

        const decodedToken = decodeJwtToken(token);
        console.log('Decoded Token:', decodedToken);

        if (!decodedToken || !decodedToken.email) {
            console.error('Invalid token or missing email');
            return;
        }

        const updateStatus = () => {
            axios.post('/api/updateStatus', {
                email: decodedToken.email,
                newStatus: 'Offline'
            })
                .then(response => {
                    console.log("Status updated successfully:", response.data);
                })
                .catch(error => {
                    console.error('Error updating status:', error);
                });
        };

        updateStatus();
    };

    const handleDownload = (fileType) => {
        const token = localStorage.getItem('token');
        axios.get('/api/getdata', {
            params: { date: dateValue },
            headers: { Authorization: `Bearer ${token}` }
        })
            .then(response => {
                const data = response.data;
                if (fileType === 'Excel') {
                    const ws = XLSX.utils.json_to_sheet(data);
                    const wb = XLSX.utils.book_new();
                    XLSX.utils.book_append_sheet(wb, ws, "Data");
                    XLSX.writeFile(wb, "Report.xlsx");
                    // } else if (fileType === 'PDF') {
                    //     // Logic to generate PDF

                    // } else {
                    console.error('Invalid fileType:', fileType);
                }
            })
            .catch(error => {
                console.error('Error fetching data:', error);
            });
    };

    const toggleDropdown = () => {
        setShowDropdown(!showDropdown);
        // setShowDropdown(false); 
    };

    const toggleDropdown2 = () => {
        setShowDropdown2(!showDropdown2);
        // setShowDropdown(false); 
    };


    const handleDownloadpdf = () => {
        const doc = new jsPDF()
        //Title 
        doc.setFontSize(16);
        doc.text("Spectron_ONGC Report", 105, 18, { align: "center" });

        // Add date/time
        doc.setFontSize(12);
        doc.text(new Date().toLocaleString(), 10, 10);


        autoTable(doc, { html: '#Static-Table', theme: 'grid', styles: { halign: 'center' }, margin: { top: 25 } })
        doc.save('Live Table.pdf')
    }
    // const [isOpen, setIsOpen] = useState(false);

    // const toggleDropdown2 = (e) => {
    //     e.preventDefault();
    //     setIsOpen(!isOpen);
    // };


    return (
        <div>

            <nav>
                <div class="left-components">
                    <img class="imgs imgsL" src="./Images/ONGC_3.jpg" alt="ONGC Logo" />
                </div>
                <div class="left-components">

                    <div class="diveup">
                        <p><b>BOILER NO : SGMB/SB/8.5T/025</b></p>
                    </div>

                    <div class="divedown">
                        <p ><b>BOILER MODEL : SGMB/8500/175</b></p>
                    </div>


                </div>
                {/* -------------------------------------------------------------- */}

                {/* --------------------------------------------------------------------- */}
                <div class="right-components">

                    <img class="imgs" src="./Images/ONGC_1.png" alt="ONGC Logo" />
                </div>
                {/* ------------------------------------------------------------------- */}
                <div class="right-components ongcbtnmain" onClick={handleLogoutButtonClick}>
                    <a href="/" class="text-sm text-blue-700 dark:text-blue-600  ">
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 inline" fill="none" viewBox="0 0 24 24" stroke="red">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18.36 6.64a9 9 0 1 1-12.73 0M12 2v10M12 12h.01M12 18v.01"></path>
                        </svg>
                    </a>

                </div>
                <div className="User_Button" onClick={handleButtonClick}>
                    <BiSolidUserRectangle />
                </div>

                {/* <div class="Admin_Button">
                    <a href="/Employee" >
                        <BiSolidUserRectangle />
                    </a>
                </div> */}

            </nav>


            {/* ================================================================================ */}



            <nav class="RM2">
                <div class="max-w-screen-xl px-0 py-1 mx-auto flex">
                    <div class="flex1 items-Left top_left">
                        <ul class="flex flex-row font-medium mt-2 space-x-6 rtl:space-x-reverse text-sm no-margin-left">

                            <li>
                                <Link to="/LiveReports" class="dashboard-button">Overview_Dashboard</Link>
                            </li>
                            <li>
                                <Link to="/Report" class="dashboard-button" >Analytics_Dashboard</Link>
                            </li>
                            <a class="dashboard-button tempbar " onClick={toggleDropdown2}>
                                Temperature
                            </a>
                            {showDropdown2 && (
                                <ul className="dropdown2-menu">
                                    <li>
                                        <Link to="/Hourly" className="block px-4 py-2 text-gray-900">Hour Wise</Link>
                                    </li>
                                    <li>
                                        <Link to="/Minutely" className="block px-4 py-2 text-gray-900 ">Minute Wise</Link>
                                    </li>

                                </ul>
                            )}
                            {/* <li>
                                <a href="/AutomaticLoadingTable" class="text-gray-900 dark:text-white hover:blink">A</a>
                            </li> */}
                            <li>
                                <Link to="/StaticTable" class="dashboard-button" aria-current="page">Report</Link>
                            </li>

                        </ul>
                    </div>
                    {/* <div class="max-w-screen-sm mx-auto flex flex1 items-Right ml-auto"> */}

                    <div class="nav2flex items-Right">
                        <div className="download-button" onClick={toggleDropdown}>
                            <BiDownload className='download' />
                            {showDropdown && (
                                <div className="dropdown-menu">
                                    <button onClick={() => handleDownload('Excel')}>Excel</button>
                                    <button onClick={(e) => { handleDownloadpdf('PDF'); e.stopPropagation(); }}>PDF</button>
                                </div>
                            )}
                        </div>
                        <input onChange={e => { dateChange(e) }} value={date} type="date" id="datetimeInput" name="datetimeInput" style={{ fontSize: '0.9rem', width: '150px', height: '27px' }} />
                    </div>
                </div>
            </nav>
        </div>
    )
}

export default Navbar