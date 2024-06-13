import React from 'react'
import { Bar, Line, Doughnut, Pie, Radar } from 'react-chartjs-2';
// import { Chart as ChartJS, defaults } from "chart.js/auto";
import { defaults } from "chart.js/auto";

// utility start //

const ChartComponent = (chartType, chartData, chartOptions) => {
    switch (chartType) {
        case 'bar':
            return <Bar data={chartData} options={chartOptions} />;
        case 'line':
            return <Line data={chartData} options={chartOptions} />;
        case 'doughnut':
            return <Doughnut data={chartData} options={chartOptions} />;
        case 'pie': 
            return <Pie data={chartData} />;
        case 'gauge':
            return <Radar data={chartData} options={chartOptions} />;
        default:
            return null;
    }
};
defaults.maintainAspectRatio = true;
defaults.responsive = true;
defaults.plugins.title.display = true;
defaults.plugins.title.align = "center";
defaults.plugins.title.font.size = 23;
defaults.plugins.title.color = "black";
// utilites end//
// component start //
function Chart(props) {

    // const barThickness = props.data.datasets && props.data.datasets.length === 1 ? 0.5 : undefined;

    // // Set the barThickness in the chart options
    // const options = {
    //     ...props.options,
    //     scales: {
    //         ...props.options?.scales,
    //         x: {
    //             ...props.options?.scales?.x,
    //             barThickness: barThickness,
    //         },
    //     },
    // };

//new method fro reducing bar sixze//
// const categoryPercentage = props.data.datasets && props.data.datasets.length === 1 ? 0.5 : 0.8;

// // Set the categoryPercentage in the chart options
// const options = {
//     ...props.options,
//     scales: {
//         ...props.options?.scales,
//         x: {
//             ...props.options?.scales?.x,
//             categoryPercentage: categoryPercentage,
//         },
//     },
// };

    



    return (
        <div>
                {ChartComponent(props.chartType,props.data,props.options)}
        </div>
    )
}
//component end //
export default Chart
