// import React, { useState } from 'react';
// import './Modal.css'; // Import CSS for modal styles

// function Updatepassword({ isOpen, onClose, onUpdate }) {
//   const [newPassword, setNewPassword] = useState('');
//   const handleUpdate = () => {
//     onUpdate(newPassword);
//     onClose();
//   };

//   return (
//     <div className={`modal ${isOpen ? 'open' : ''}`}>
//       <div className="modal-content">
//         <span className="close" onClick={onClose}>&times;</span>
//         <h2>Update Password</h2>
//         <input
//           type="password"
//           placeholder="New Password"
//           value={newPassword}
//           onChange={(e) => setNewPassword(e.target.value)}
//         />
//         <button onClick={handleUpdate}>Update</button>
//       </div>
//     </div>
//   );
// }

// export default Updatepassword;
