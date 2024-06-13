import { configureStore } from '@reduxjs/toolkit';
import dateManagerReducer from './dateManager';

export default configureStore({
    reducer:{
        dateManager:dateManagerReducer,
    },
})