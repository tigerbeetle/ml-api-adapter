const Kafka = require('@mojaloop/central-services-stream').Util
const uuid = require('uuid')
const util = require('util')
const config = require('../lib/config')


/**
 * @class MessageBatcher
 * @description Responsible for batching together fulfil or prepare messages to be sent in a single
 *   Kafka Message
 */
class MessageBatcher {
  _producer
  _batchSizePrepare
  _batchIntervalPrepare
  _batchSizeFulfil
  _batchIntervalFulfil

  // TODO (LD): document
  // private _transferQueue: { transfer: Transfer; resolve: () => void; reject: (error: any) => void }[] = [];

  // A list of messages along with promises to be shipped
  _prepareQueue = []
  _fulfilQueue = []
  
  _timerPrepare
  _timerFulfil


  constructor(producer, batchSizePrepare, batchIntervalPrepare, batchSizeFulfil, batchIntervalFulfil) {
    this._producer = producer
    this._batchSizePrepare = batchSizePrepare
    this._batchIntervalPrepare = batchIntervalPrepare
    this._batchSizeFulfil = batchSizeFulfil
    this._batchIntervalFulfil = batchIntervalFulfil

    // Send off the batches in an event loop or something
    this._timerPrepare = setInterval(() => this.flushPrepareQueue(), this._batchIntervalPrepare)
    this._timerFulfil = setInterval(() => this.flushFulfilQueue(), this._batchIntervalFulfil)
  }

  /**
   * Adds a _prepare_ message to the batcher ready to be sent. I'm trying to not
   * make this too generic at this stage, but eventually we might be able to
   */
  async enqueuePrepare(prepare) {
    return new Promise((resolve, reject) => {
      this._prepareQueue.push({prepare, resolve, reject})

      if (this._prepareQueue.length >= this._batchSizePrepare) {
        this.flushPrepareQueue()
      }
    })
  }
  
  /**
   * 
   * @param {*} fulfil 
   * @param {*} metadata 
   * @param {string} metadata.transferId
   * @param {string} metadata.payerFsp
   * @param {string} metadata.payerFsp
   * @returns 
   */
  async enqueueFulfil(fulfil, metadata) {
    return new Promise((resolve, reject) => {
      this._fulfilQueue.push({fulfil, metadata, resolve, reject})

      if (this._fulfilQueue.length >= this._batchSizeFulfil) {
        this.flushFulfilQueue()
      }
    })
  }

  flushPrepareQueue() {
    if (this._prepareQueue.length === 0) {
      return
    }

    const batch = this._prepareQueue.splice(0, this._batchSizePrepare);
    console.log(`MessageBatcher - shipping batch of size: ${batch.length} to ${"transfer-batch-prepare"}`)

    const messageProtocol = {
      content: {
        count: batch.length,
        batch: batch.map(message => message.prepare),
      },
      id: uuid.v4()
    }
    const topicConf = {
      topicName: 'transfer-batch-prepare'
    }
    // Catch async errors explicitly so we don't accidentally miss them
    this._producer.produceMessage(messageProtocol, topicConf)
    .then(() => {
      // iterate through sent messages and resolve
      batch.forEach(message => message.resolve())
    })
    .catch(err => {
      console.log(`MessageBatcher - async error producing message: ${util.inspect(err)}`)
      batch.forEach(message => message.reject())
    })
  }

  flushFulfilQueue() {
    if (this._fulfilQueue.length === 0) {
      return
    }

    const batch = this._fulfilQueue.splice(0, this._batchSizeFulfil);
    console.log(`MessageBatcher - shipping batch of size: ${batch.length} to ${"transfer-batch-fulfil"}`)

    // TODO: need to handle this differently to prepares
    const messageProtocol = {
      content: {
        count: batch.length,
        batch: batch.map(message => message.fulfil),
        metadata: batch.map(message => message.metadata),
      },
      id: uuid.v4()
    }
    const topicConf = {
      topicName: 'transfer-batch-fulfil'
    }
    // Catch async errors explicitly so we don't accidentally miss them
    this._producer.produceMessage(messageProtocol, topicConf)
    .then(() => {
      // iterate through sent messages and resolve
      batch.forEach(message => message.resolve())
    })
    .catch(err => {
      console.log(`MessageBatcher - async error producing message: ${util.inspect(err)}`)
      batch.forEach(message => message.reject())
    })
  }
}

const messageBatcher = new MessageBatcher(
  Kafka.Producer, 
  config.KAFKA.DEBUG_EXTREME_BATCHING_PREPARE_BATCH_SIZE,
  config.KAFKA.DEBUG_EXTREME_BATCHING_PREPARE_LINGER_MS,
  config.KAFKA.DEBUG_EXTREME_BATCHING_FULFIL_BATCH_SIZE,
  config.KAFKA.DEBUG_EXTREME_BATCHING_FULFIL_LINGER_MS,
)

module.exports = messageBatcher