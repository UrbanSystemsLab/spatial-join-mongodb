// node init.js --db 'mongodb://localhost:27017/nyc' --innerLayer buildings --outerLayer lots --outputLayer buildings_spatialJoin --outerLayerAttributes 'borough'

const argv = require('yargs').array('outerLayerAttributes').argv
const MongoClient = require('mongodb').MongoClient
const cluster = require('cluster')
const numCPUs = require('os').cpus().length
const async = require('async')

const mongoUrl = argv.db // 'mongodb://localhost:27017/nyc'
const innerLayerCollection = argv.innerLayer // 'buildings'
const outerLayerCollection = argv.outerLayer // 'lots'
const outerLayerAttributes = argv.outerLayerAttributes // 'borough code'
const outputLayerCollection = argv.outputLayer // 'buildings_spatialJoin'

var workerBatchSize = 1000 // Size of features to be sent at once by master to workers for processing

var childProcessPid = [] // Keep track of Child Processes PIDs
var featureCursor
var db = null
var dbCollections = []

if (innerLayerCollection && outerLayerCollection && outputLayerCollection) {
	if (innerLayerCollection === outerLayerCollection || outerLayerCollection === outputLayerCollection || innerLayerCollection === outputLayerCollection) {
		console.error(`Error: Input different layer collection names`)
		process.exit()
	}
	// Begin the join process
	init()
} else {
	console.error(`Invalid Arguments`)
	console.log(`Example: node init.js --db 'mongodb://localhost:27017/db' --innerLayer buildings --outerLayer lots --outputLayer buildings_spatialJoin --outerLayerAttributes 'borough'`)
}

function init() {
	if (cluster.isMaster) {
		console.log(`MongoDB URL: ${mongoUrl}`)
		console.log(`Inner Layer: ${innerLayerCollection}`)
		console.log(`Outer Layer: ${outerLayerCollection}`)
		console.log(`Output Layer: ${outputLayerCollection}`)
		MongoClient.connect(mongoUrl)
			.then(_db => {
				console.log('Connected to DB')
				console.time('spatialJoin')
				db = _db // Make it available globally for Master
				return db
			})
			.then((db) => {
				return db.collection(outerLayerCollection).count()
			})
			.then((count) => {
				// Adjust the worker batch size 
				adjustWorkerBatchSize(count)
				// Also fetch all the collection names in the database
				return db.listCollections().toArray()
			})
			.then(() => {
				featureCursor = db.collection(outerLayerCollection).find()
			})
			.catch(err => {
				console.error('Could not connect to DB ', err)
			})

		// Start workers
		for (let i = 0; i < numCPUs; i++) {
			var master = cluster.fork()
			childProcessPid.push(master.process.pid)
		}

		// Message listener for Master from each worker
		for (var id in cluster.workers) {
			cluster.workers[id].on('message', (msg) => { messageHandler(msg) })
		}

		// Master queue to iterate over a batch of features
		var qMaster = async.queue((worker, callback) => {
			// Send Next Batch of Features
			sendFeatureBatchToWorker(worker, () => {
				callback()
			})
		}, 1)

		qMaster.drain = function() {
			// Do nothing
			console.log('Master queue drained. Waiting for request from workers.')
		}

		// Handle incoming messages from workers
		var messageHandler = function(msg) {
			if (msg.action === 'request') {
				// Determine which worker is requesting next batch of features
				for (const id in cluster.workers) {
					if (cluster.workers[id].process.pid === msg.pid) {
						qMaster.push(cluster.workers[id], (err) => { if (err) console.error(err) })
					}
				}
			}
		}
	}

	if (cluster.isWorker) {
		var dbWorker = null
		MongoClient.connect(mongoUrl)
			.then(_db => {
				dbWorker = _db // Make it available globally within Worker
				return null
			})
			.then(() => {
				// Ask for first batch of features
				console.log(`Worker process ${process.pid} started`)
				process.send({ action: 'request', pid: process.pid })
			})
			.catch(err => { console.error('Could not connect to DB ', err) })

		// Spatial join worker queue of concurrency 10
		var qWorker = async.queue((outerLayerFeature, callback) => {
			// -------------------------------------
			// Find inner layer features and perform the join
			// -------------------------------------
			dbWorker.collection(innerLayerCollection).find({
				'geometry': {
					'$geoWithin': {
						'$geometry': {
							type: outerLayerFeature.geometry.type,
							coordinates: outerLayerFeature.geometry.coordinates
						}
					}
				}
			}).toArray((err, innerLayerFeatures) => {
				if (err) { console.error(`Error finding inner features: ${err}`) }
				async.eachSeries(innerLayerFeatures, (innerLayerFeature, callback) => {
					// Output the join to outputLayerCollection
					for (var i = 0; i < outerLayerAttributes.length; i++) {
						innerLayerFeature.properties[outerLayerAttributes[i]] = outerLayerFeature.properties[outerLayerAttributes[i]]
					}
					delete innerLayerFeature._id
					dbWorker.collection(outputLayerCollection).insert(innerLayerFeature, (err, res) => {
						if (err) callback(err)
						else callback() // Successful insert
					})
				}, () => {
					callback() // Inserted all found features
				})
			})
			// -------------------------------------
		}, 10)

		// Request master for more features
		qWorker.drain = function() {
			// Send a message back to master
			console.log(process.pid, 'batch processed')
			process.send({ action: 'request', pid: process.pid })
		}

		// Receive messages from the master process.
		process.on('message', (msg) => {
			if (msg.data) {
				// Rx some data from master and add it to the queue
				qWorker.push(msg.data, (err) => { if (err) console.error(`Worker Queue Error: ${err}`) })
			}
		})
	}

	cluster.on('exit', (worker, code, signal) => {
		console.log(`worker ${worker.process.pid} died`)
	})
}

// Send a batch of features to worker thread for processing
function sendFeatureBatchToWorker(worker, callback) {
	var batchCount = 0
	nextFeature(worker)

	function nextFeature(worker) {
		// console.log('worker ', worker.process.pid)
		featureCursor.nextObject((err, feature) => {
			if (!err && (feature !== null)) {
				worker.send({ data: feature })
				if (batchCount <= workerBatchSize) {
					batchCount++
					process.nextTick(() => {
						nextFeature(worker)
					})
				} else {
					callback() // End of batch
				}
			} else if (err) {
				console.error(`Master Queue Error: ${err}`)
				process.exit()
			} else {
				// TODO: Check if other workers are done. Only then quit the application
				console.timeEnd('spatialJoin')
				console.log('Master queue exhausted')
				// process.exit()
			}
		})
	}
}

function adjustWorkerBatchSize(count) {
	if (count < 10) {
		workerBatchSize = 1
	} else if (count < 100) {
		workerBatchSize = 10
	} else {
		workerBatchSize = 1000
	}
}