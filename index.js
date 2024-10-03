const { Client } = require('@elastic/elasticsearch');
const fs = require('fs'); //file system for read and write
const csv = require('csv-parser');

// Connect to Elasticsearch
const client = new Client({ node: 'http://localhost:9200' });

// Define the index name
const indexName = 'employee_data';

// asynchronus function to create an index 
async function createIndex() {
  const indexExists = await client.indices.exists({ index: indexName });

  //checks if the index is already exits or not
  if (!indexExists.body) {   
        await client.indices.create({ index: indexName });
    console.log(`Index '${indexName}' created.`);
  } else {
    console.log(`Index '${indexName}' already exists.`);
  }
}

//function to index data from CSV
async function indexData() {
  const csvFilePath = "C:\Users\sunda\Desktop\employee_datas\employee_data.csv";  //path

  fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on('data', async (row) => {
      await client.index({
        index: indexName,
        body: row,
      });
    })
    .on('end', () => {
      console.log(`Data indexed successfully into '${indexName}'!`);
    });
}

//run the functions
createIndex().then(indexData);
