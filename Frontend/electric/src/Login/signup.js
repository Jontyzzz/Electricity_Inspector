import React, { useState } from 'react';
import axios from 'axios';
import { Link, useNavigate } from 'react-router-dom';
import validation from '../Utility/signupvalidation';
import sha256 from 'crypto-js/sha256'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faEye, faEyeSlash } from '@fortawesome/free-solid-svg-icons';


function Signup() {
  const [values, setValues] = useState({
    name: '',
    email: '',
    password: ''
  });
  const [error, setErrors] = useState({}); // Corrected from 'setErrors' to 'setErrors'
  const [showPassword, setShowPassword] = useState(false); // State to track password visibility
  const navigate = useNavigate();

  const handleInput = (event) => {
    setValues({ ...values, [event.target.name]: event.target.value });
    setErrors(validation({ ...values, [event.target.name]: event.target.value })); // Corrected from 'setError' to 'setErrors'
  };

  // const handleSubmit = (event) => {
  //   event.preventDefault();
  //   setErrors(validation(values))
  //   // if (Object.keys(error).length === 0) {
  //   if (error.name === "" && error.email === "" && error.password === "") {
  //     setValues(prev => ({
  //       ...prev, password: sha256(values.password)
  //     }))
  //     console.log(values)
  //     axios.post('/api/signup', values)
  //       .then(res => {
  //         console.log("Registered successfully...");
  //         navigate('/');
  //       })
  //       .catch(err => console.log(err));
  //   }
  // };

  const handleSubmit = (event) => {
    event.preventDefault();
    setErrors(validation(values));
  
    if (error.name === "" && error.email === "" && error.password === "") {
      const hashedPassword = sha256(values.password).toString(); // Convert hashed password to string
      console.log('Hashed Password:', hashedPassword);
      setValues(prev => ({ ...prev, password: hashedPassword }));
  
      console.log(values);
      axios.post('/api/signup', values)
        .then(res => {
          console.log("Registered successfully...");
          navigate('/');
        })
        .catch(err => console.log(err));
    }
  };
  
  const togglePasswordVisibility = () => {
    setShowPassword(prevState => !prevState);
  };

  

  return (
    <div className='Signup template d-flex justify-content-center w-100 vh-100 justify-content-center align-items-center containerStyle'>
      <div className='bg-white p-5 rounded upacity'>
        <form onSubmit={handleSubmit}>
          <h2>Sign-up</h2>
          <div className='mb-3'>
            <label htmlFor='name'><strong>Name</strong></label>
            <input type='text' placeholder='Enter name' name='name' className='form-control rounded-0' onChange={handleInput} />
            {error.name && <span className='text-danger'>{error.name}</span>}
          </div>
          <div className='mb-3'>
            <label htmlFor='email'><strong>Email</strong></label>
            <input type='email' placeholder='Enter Email' name='email' className='form-control rounded-0' onChange={handleInput} />
            {error.email && <span className='text-danger'>{error.email}</span>}
          </div>
          <div className='mb-3'>
            <label htmlFor='password'><strong>Password</strong></label>
            <div className='input-group'>
              <input type={showPassword ? 'text' : 'password'} title='Password must contain at least 8 characters, including one uppercase letter, one lowercase letter, and one number'
                placeholder='Enter password' name='password' className='form-control rounded-0' onChange={handleInput} />
              <button type='button' className='btn btn-outline-secondary' onClick={togglePasswordVisibility}>
                <FontAwesomeIcon icon={showPassword ? faEyeSlash : faEye} />
              </button>
            </div>
            {error.password && <span className='text-danger'>{error.password}</span>}
          </div>
          <button type='submit' className='btn btn-success w-100 rounded-0'>Signup</button>
          <h6 className='Paragraph'>You agree to the terms and policies.</h6>
          <Link to='/' className='btn btn-default border w-100 bg-light rounded-0 text-decoration-none'>Back to Login</Link>
        </form>
      </div>
    </div>
  );
}

export default Signup;
