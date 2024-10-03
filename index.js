const { Client } = require('@elastic/elasticsearch');
const fs = require('fs'); // File system for reading and writing files
const csv = require('csv-parser');

// Connect to Elasticsearch
const client = new Client({ node: 'http://127.0.0.1:9200' });

// Define the index name
const indexName = 'employee_data';

// Asynchronous function to create an index
async function createIndex() {
    const indexExists = await client.indices.exists({ index: indexName });

    // Checks if the index already exists
    if (!indexExists.body) {
        await client.indices.create({ index: indexName });
        console.log(`Index '${indexName}' created.`);
    } else {
        console.log(`Index '${indexName}' already exists.`);
    }
}

// Function to index data from CSV
async function indexData(p_exclude_column) {
    const csvFilePath = 'C:/Users/sunda/Desktop/employee_datas/employee_data.csv';  
    fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', async (row) => {
            if (p_exclude_column in row) {
                delete row[p_exclude_column]; // Exclude the specified column
            }
            await client.index({
                index: indexName,
                body: row,
            });
        })
        .on('end', () => {
            console.log(`Data indexed successfully into '${indexName}', excluding column '${p_exclude_column}'!`);
        });
}

// Function to search by a specific column and value
async function searchByColumn(p_column_name, p_column_value) {
    const { body } = await client.search({
        index: indexName,
        body: {
            query: {
                match: {
                    [p_column_name]: p_column_value,
                }
            }
        }
    });
    console.log(`Search results for ${p_column_name} = ${p_column_value}:`);
    body.hits.hits.forEach(hit => console.log(hit._source));
}

// Function to get the total count of employees in the collection
async function getEmpCount() {
    const { body } = await client.count({ index: indexName });
    console.log(`Total employees in '${indexName}': ${body.count}`);
}

// Function to delete an employee by ID
async function delEmpById(p_employee_id) {
    const { body } = await client.deleteByQuery({
        index: indexName,
        body: {
            query: {
                match: { emp_id: p_employee_id },
            }
        }
    });
    console.log(`Deleted employee with ID: ${p_employee_id}`);
}

// Function to retrieve department facet (employee count grouped by department)
async function getDepFacet() {
    const { body } = await client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                departments: {
                    terms: {
                        field: "department.keyword"
                    }
                }
            }
        }
    });

    console.log("Department Facet Results:");
    body.aggregations.departments.buckets.forEach(bucket => {
        console.log(`${bucket.key}: ${bucket.doc_count} employees`);
    });
}

// Execute functions
async function executeFunctions() {
    const v_nameCollection = 'Hash_AnnamalaiR'; 
    const v_phoneCollection = 'Hash_8064'; 

    // 1. Create index
    await createIndex();

    // 2. Get employee count before indexing
    await getEmpCount();

    // 3. Index data, excluding specific columns
    await indexData('Department'); // Exclude the 'Department' column
    await indexData('Gender'); // Exclude the 'Gender' column

    // 4. Delete an employee by ID
    await delEmpById('E02003'); // This is  actual employee ID

    // 5. Get employee count after deletion
    await getEmpCount();

    // 6. Search by column values
    await searchByColumn('Department', 'IT');
    await searchByColumn('Gender', 'Male');

    // 7. Get department facets (group employees by department)
    await getDepFacet();
}

// Run the function to execute everything
executeFunctions().catch(console.error);
