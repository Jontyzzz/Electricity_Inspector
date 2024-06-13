
function validation(values) {
    let error = {};
    const email_pattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const password_pattern = /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*()-_+=<>?]).{8,}$/;
  
    if (values.email === "") {
      error.email = "Name should not be Empty";
    } else if (!email_pattern.test(values.email)) {
      error.email = "Email Didn't match";
    } else {
      error.email = "";
    }
  
    if (values.password === "") {
      error.password = "Password shouldn't be Empty";
    } else if (!password_pattern.test(values.password)) {
      error.password = "Password Didn't match";
    } else {
      error.password = "";
    }
  
    return error;
  }
  
  export default validation;
