
import React, { useState } from 'react';
import { Link, Navigate, useNavigate } from 'react-router-dom';
import axios from 'axios';
import validation from '../Utility/loginvalidation';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faEye, faEyeSlash } from '@fortawesome/free-solid-svg-icons';


function Login() {
    const [values, setValues] = useState({
        email: '',
        password: ''
    });

    const [showPassword, setShowPassword] = useState(false); // State to track password visibility


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

    // const navigate = useNavigate();
    const [passwordVisible, setPasswordVisible] = useState(false);
    const [password, setPassword] = useState('');
    const [isLogged, setIsLogged] = useState(1)
    const [error, setError] = useState({});
    const handleInput = (event) => {
        setValues(prev => ({ ...prev, [event.target.name]: event.target.value }));

    }



    const handleSubmit = async (event) => {
        event.preventDefault();
        const validationErrors = validation(values); // Perform validation
        setError(validationErrors); // Update error state

        // Check if there are no validation errors
        if (Object.values(validationErrors).every(error => error === "")) {
            try {
                const res = await axios.post('/api/login', values);
                console.log("Form Input :", values);
                console.log("/api/login Response :", res.data);
                localStorage.setItem("token", res.data.token);

                // Decode token to extract user information
                const decodedToken = decodeJwtToken(res.data.token);
                console.log('Decoded Token:', decodedToken);

                if (!decodedToken || !decodedToken.email) {
                    console.error('Invalid token or missing email');
                    return;
                }

                // Update status in the remote MySQL database
                const response = await axios.post('/api/updateStatus', {
                    email: decodedToken.email,
                    newStatus: 'Online'
                });
                console.log("Status updated successfully:", response.data);
                setIsLogged(2); // Set isLogged state to trigger navigation
            } catch (err) {
                console.error(err);
                alert("Please try login again. Either your Password or Email is Incorrect");
            }
        }
    };

    const togglePasswordVisibility = () => {
        setShowPassword(prevState => !prevState);
    };

    if (isLogged === 2) {
        return (<Navigate to="/LiveReports"></Navigate>)
    }
    return (
        <div className='Signup template d-flex justify-content-center w-100 vh-100  justify-content-center align-items-center containerStyle'>
            <div className='bg-white p-5 rounded  upacity'>
                <form onSubmit={handleSubmit}>
                    <h2>Login</h2>
                    <div className='mb-3'>
                        <label htmlFor='email'><strong>Email</strong></label>
                        <input
                            type='email'
                            placeholder='Enter Email'
                            name='email'
                            className='form-control rounded-0'
                            onChange={handleInput}
                        />
                        {error.email && <span className='text-danger'>{error.email}</span>}
                    </div>
                    <div className='mb-3'>
                        <label htmlFor='password'> <strong>Password</strong></label>
                        <div className='input-group'>
                            <input type={showPassword ? 'text' : 'password'}
                                placeholder='Enter password'
                                name='password'
                                className='form-control rounded-0'
                                onChange={handleInput}
                            />
                            <button type='button' className='btn btn-outline-secondary' onClick={togglePasswordVisibility}>
                                <FontAwesomeIcon icon={showPassword ? faEyeSlash : faEye} />
                            </button>
                        </div>
                        {error.password && <span className='text-danger'>{error.password}</span>}
                    </div>
                    <h6 className='Paragraph'>You agree to the terms and conditions + policies</h6>
                    <button type='submit' className='btn btn-success w-100 rounded-0 text-decoration-none'>Login.</button><h6 className='Paragraph'>you need to create account</h6>
                    <Link to='/signup' className='btn btn-default border w-100 bg-light rounded-0 text-decoration-none'>Create Account</Link>
                </form>
            </div>
        </div>
    );
}

export default Login;

