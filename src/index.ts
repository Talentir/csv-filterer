import { program } from "commander";
import fs from "fs";
import * as readline from "node:readline";
import ProgressBar from "progress";

program
	.name("talentir")
	.description("CLI to process huge Youtube CSV reports")
	.version("0.1.0");

program
	.command("filter")
	.description("Filter YouTube Reports")
	.requiredOption("-i, --input <input>", "Input file")
	.option("-o, --output <output>", "Output file")
	.option(
		"-s, --separator <separator>",
		"The separator used in the input file",
		",",
	)
	.action(async (commandAndOptions) => {
		if (commandAndOptions.input == null)
			program.error("--input option is required");

		// check if file exists
		if (!fs.existsSync(commandAndOptions.input)) {
			program.error(`File ${commandAndOptions.input as string} does not exist`);
		}

		const outputFile = commandAndOptions.output as string | undefined;
		const outputFilePath = outputFile ?? `${commandAndOptions.input.substring(0, commandAndOptions.input.lastIndexOf('/') + 1)}filtered-${commandAndOptions.input.split('/').pop()}`;
		console.log(outputFilePath);

		const readStream = fs.createReadStream(commandAndOptions.input);
		const writeStream = fs.createWriteStream(outputFilePath);

		const totalSize = fs.statSync(commandAndOptions.input).size;

		// Fetch channel IDs from API
		const response = await fetch('https://talentir.com/api/youtube-channel-ids');
		if (!response.ok) {
			program.error('Failed to fetch channel IDs from API');
		}
		
		const channelIds = (await response.json()).youtubeChannelIds as string;
		const channelIdsArray = channelIds.split(",");

		// Convert comma-separated string into array of unique channel IDs, including non-UC variants
		const uniqueChannelIds = [...new Set(channelIdsArray.flatMap(id => {
			if (id.startsWith('UC')) {
				return [id, id.substring(2)]; // Include both UC and non-UC versions
			}
			return [id, `UC${id}`]; // Include both non-UC and UC versions
		}))];

		const bar = new ProgressBar(":bar, :percent, :elapseds, :etas", {
			total: 100,
		});

		let transferredSize = 0;
		readStream.on("data", (chunk) => {
			transferredSize += chunk.length;
			const progress = transferredSize / totalSize;
			bar.update(progress);
		});

		const columns = ["Channel Asset ID", "Channel ID"]

		const columnFilters = columns.flatMap(column => {
			return uniqueChannelIds.map((assetId) => {
				return {
					column: column,
					value: assetId,
				};
			});
		});

		console.log(columnFilters);

		const rl = readline.createInterface({
			input: readStream,
			crlfDelay: Number.POSITIVE_INFINITY,
		});

		let columnIndexAndValue: Array<{
			columnIndex: number;
			columnValue: string;
		}> = [];
		let firstLine = true;

		let foundLines = 0;
		let totalNumberOfLines = 0;

		// Create Promise for rl.on
		await new Promise<void>((resolve, reject) => {
			rl.on("error", (err) => {
				reject(err);
			});
			rl.on("close", () => {
				console.log(
					"\x1b[32m%s\x1b[0m",
					`Found and wrote ${foundLines.toLocaleString()} lines from originally ${totalNumberOfLines.toLocaleString()} to "${commandAndOptions.output as string}"`,
				);
				resolve();
			});
			rl.on("line", (line) => {
				const fileValues = line.split(commandAndOptions.separator as string);

				if (firstLine) {
					writeStream.write(line + "\n");
					firstLine = false;

					// concert columnFilters into indexes
					columnIndexAndValue = columnFilters.map((filter) => {
						return {
							columnIndex: fileValues.indexOf(filter.column),
							columnValue: filter.value,
						};
					});
					return;
				}

				// check if any of the column filters match
				const found = columnIndexAndValue.some((filter) => {
					return fileValues[filter.columnIndex] === filter.columnValue;
				});

				totalNumberOfLines += 1;

				if (found) {
					foundLines += 1;
					writeStream.write(line + "\n");
				}
			});
		});
	});

program.parse(process.argv);
