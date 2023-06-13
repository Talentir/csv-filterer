import fs from "fs"
import * as readline from 'node:readline';

// ------------ EDIT HERE ------------------
// These files are in relation to where you run the script
const inputFile = "../test-file.csv"
const outputFile = "../filtered.csv"

const columnFilters = [{
    column: "Variable_code",
    value: "H04"
}]
// ------------------------------------------

const readStream = fs.createReadStream(inputFile)
const writeStream = fs.createWriteStream(outputFile)

const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity
})

let columnIndexAndValue: { columnIndex: number, columnValue: string }[] = [];
let firstLine = true;

rl.on('line', (line) => {
    const fileValues = line.split(',');

    if (firstLine) {
        writeStream.write(line + "\n")
        firstLine = false

        // concert columnFilters into indexes
        columnIndexAndValue = columnFilters.map((filter) => {
            return { 
                columnIndex: fileValues.indexOf(filter.column),
                columnValue: filter.value
            }
        })
        return
    }

    // check if any of the column filters match
    const found = columnIndexAndValue.some((filter) => {
        return fileValues[filter.columnIndex] === filter.columnValue
    })

    if (found) {
        writeStream.write(line + "\n")
        console.log("Found Line: ", line)
    }
})