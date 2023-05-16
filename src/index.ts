import fs from "fs"
import * as readline from 'node:readline';

const inputFile = "organizations-2000000.csv"
const outputFile = "filtered.csv"
const columnFilters = [{
    column: "Name",
    value: "Cuevas-Carey"
}]

const readStream = fs.createReadStream(inputFile)
const writeStream = fs.createWriteStream('filtered.csv')

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
        console.log("found line", line)
    }
})