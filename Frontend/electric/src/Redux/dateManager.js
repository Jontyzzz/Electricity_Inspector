import { createSlice } from '@reduxjs/toolkit';
import { getItem, setItem } from '../Utility/localstorage';

const date = (() => { //self calling function
    var dateOBJ = new Date()
    var Month = dateOBJ.getMonth() + 1;
    var day = dateOBJ.getDate();
    var date = dateOBJ.getFullYear() + "-" + (Month <= 9 ? "0" + Month : Month) + "-" + (day <= 9 ? "0" + day : day);
    let new_date = getItem("date");
    if (new_date !== null) {
        date = new_date;
    } else {
        setItem("date", date);
    }

    return date;
})()


export const dateManagerSlice = createSlice({
    name: "dateStore",
    initialState: {
        value: date,
    },
    reducers: {
        setDateValue: (state, action) => {
            state.value = action.payload;
            setItem("date", action.payload);
        },
    },
});

export const { setDateValue } = dateManagerSlice.actions;
export default dateManagerSlice.reducer;