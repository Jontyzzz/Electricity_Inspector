import React from 'react';
import './ErrorPage.css'; // Import CSS file for styling

const ErrorPage = ({ errorMessage }) => {
  return (
    <div className="error-page">
      <h1>Error, Something went wrong...</h1>
      <p className="error-message">{errorMessage}</p>
    </div>
  );
};

export default ErrorPage;