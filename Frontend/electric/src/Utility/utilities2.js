// function getDataset1(parameterNames, data, unit, chartType, customColors) {
//     // Initialize an array to store datasets
//     const datasets = [];
  
//     // Loop through each parameter name
//     parameterNames.forEach((paramName, index) => {
//       // Filter data for the current parameter
//       const filteredData = data.filter(dataPoint => dataPoint.ParameterName === paramName);
  
//       // Create a dataset for the current parameter
//       const dataset = {
//         type: 'line',
//         label: paramName,
//         data: filteredData.map(dataPoint => ({
//           x: dataPoint.Timestamp, // Assuming your data has a Timestamp property for the x-axis
//           y: parseFloat(dataPoint.ParameterValue),
//         })),
//         borderColor: customColors[index % customColors.length] || "rgba(43, 63, 229, 1)",
//         borderWidth: 2,
//         pointRadius: 4,
//         hoverOffset: 3,
//       };
  
//       // Push the dataset to the array
//       datasets.push(dataset);
//     });
  
//     return {
//       datasets,
//     };
//   }

//   module.exports = {getDataset1};
  