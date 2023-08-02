import fs from 'fs'
import * as readline from 'node:readline'
import { program } from 'commander'
import { ProgressBar } from 'ascii-progress'

program
  .name('talentir')
  .command('asset-report-filter')
  .option('-i, --input <input>', 'Input file')
  .option('-o, --output <output>', 'Output file')
  .action(async (commandAndOptions) => {
    if (commandAndOptions.input == null) program.error('--input option is required')
    const outputFile = commandAndOptions.output ?? 'filtered.csv'

    // check if file exists
    if (!fs.existsSync(commandAndOptions.input)) {
      program.error(`File ${commandAndOptions.input} does not exist`)
    }

    const readStream = fs.createReadStream(commandAndOptions.input)
    const writeStream = fs.createWriteStream(outputFile)

    const totalSize = fs.statSync(commandAndOptions.input).size

    const bar = new ProgressBar({
      schema: ':bar.brightCyan, :percent.bold, :elapseds, :etas'
    })

    let transferredSize = 0
    readStream.on('data', (chunk) => {
      transferredSize += chunk.length
      const progress = (transferredSize / totalSize)
      bar.update(progress)
    })

    const columnFilters = [{
      column: 'Age',
      value: '38'
    }]

    const rl = readline.createInterface({
      input: readStream,
      crlfDelay: Infinity
    })

    let columnIndexAndValue: Array<{ columnIndex: number, columnValue: string }> = []
    let firstLine = true

    let foundLines = 0

    // Create Promise for rl.on
    await new Promise<void>((resolve, reject) => {
      rl.on('error', (err) => {
        reject(err)
      })
      rl.on('close', () => {
        console.log('\x1b[32m%s\x1b[0m', `Found and wrote ${foundLines} lines to "${outputFile}"`)
        resolve()
      })
      rl.on('line', (line) => {
        const fileValues = line.split(',')

        if (firstLine) {
          writeStream.write(line + '\n')
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
          foundLines += 1
          writeStream.write(line + '\n')
        }
      })
    })
  })

program.parse(process.argv)
