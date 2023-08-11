import fs from 'fs'
import * as readline from 'node:readline'
import { program } from 'commander'
import ProgressBar from 'progress'

program
  .name('talentir')
  .description('CLI to process huge Youtube CSV reports')
  .version('0.1.0')

program
  .command('asset-report-filter')
  .description('Filter asset report')
  .requiredOption('-i, --input <input>', 'Input file')
  .option('-o, --output <output>', 'Output file', 'filtered.csv')
  .option('-c, --asset-id-column <column>', 'The name of the asset-id-column', 'Asset ID')
  .option('-s, --separator <separator>', 'The separator used in the input file', ',')
  .requiredOption('-a, --asset-ids <value>', 'Asset IDs to filter, comma separated')
  .action(async (commandAndOptions) => {
    if (commandAndOptions.input == null) program.error('--input option is required')

    // check if file exists
    if (!fs.existsSync(commandAndOptions.input)) {
      program.error(`File ${commandAndOptions.input as string} does not exist`)
    }

    const readStream = fs.createReadStream(commandAndOptions.input)
    const writeStream = fs.createWriteStream(commandAndOptions.output)

    const totalSize = fs.statSync(commandAndOptions.input).size

    const bar = new ProgressBar(':bar, :percent, :elapseds, :etas', { total: 100 })

    let transferredSize = 0
    readStream.on('data', (chunk) => {
      transferredSize += chunk.length
      const progress = (transferredSize / totalSize)
      bar.update(progress)
    })

    const assetIds = (commandAndOptions.assetIds as string).split(',')
    const columnFilters = assetIds.map((assetId) => {
      return {
        column: commandAndOptions.assetIdColumn as string,
        value: assetId
      }
    })

    const rl = readline.createInterface({
      input: readStream,
      crlfDelay: Infinity
    })

    let columnIndexAndValue: Array<{ columnIndex: number, columnValue: string }> = []
    let firstLine = true

    let foundLines = 0
    let totalNumberOfLines = 0

    // Create Promise for rl.on
    await new Promise<void>((resolve, reject) => {
      rl.on('error', (err) => {
        reject(err)
      })
      rl.on('close', () => {
        console.log('\x1b[32m%s\x1b[0m', `Found and wrote ${foundLines.toLocaleString()} lines from originally ${totalNumberOfLines.toLocaleString()} to "${commandAndOptions.output as string}"`)
        resolve()
      })
      rl.on('line', (line) => {
        const fileValues = line.split(commandAndOptions.separator as string)

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

        totalNumberOfLines += 1

        if (found) {
          foundLines += 1
          writeStream.write(line + '\n')
        }
      })
    })
  })

program.parse(process.argv)
