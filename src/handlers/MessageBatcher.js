const Kafka = require('@mojaloop/central-services-stream').Util
const uuid = require('uuid')
const util = require('util')

class MessageBatcher {
  _producer
  _batchSize
  _batchInterval

  // private _transferQueue: { transfer: Transfer; resolve: () => void; reject: (error: any) => void }[] = [];

  // A list of messages along with promises to be shipped
  // TODO: mutiple queues?
  _messageQueue = []


  constructor(producer, batchSize, batchInterval) {
    this._producer = producer
    this._batchSize = batchSize
    this._batchInterval = batchInterval

    // Send off the batches in an event loop or something
    this._timer = setInterval(() => this.flushQueue(), this._batchInterval)
  }


  /**
   * Adds a _prepare_ message to the batcher ready to be sent. I'm trying to not
   * make this too generic at this stage, but eventually we might be able to
   */
  async enqueuePrepare(prepare) {
    return new Promise((resolve, reject) => {
      this._messageQueue.push({prepare, resolve, reject})

      if (this._messageQueue.length >= this.batchSize) {
        this.flushQueue()
      }
    })
  }


  flushQueue() {
    if (this._messageQueue.length === 0) {
      return
    }

    const batch = this._messageQueue.splice(0, this._batchSize);
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
}

// const messageBatcher = new MessageBatcher(Kafka.Producer, 4000, 100);
const messageBatcher = new MessageBatcher(Kafka.Producer, 5, 100);

module.exports = messageBatcher