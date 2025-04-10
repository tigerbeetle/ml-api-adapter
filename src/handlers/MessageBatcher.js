const uuid = require('uuid')

class MessageBatcher {
  _producer
  _batchSize
  _batchInterval

  // private _transferQueue: { transfer: Transfer; resolve: () => void; reject: (error: any) => void }[] = [];

  // A list of messages along with promises to be shipped
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

    const content = this._messageQueue.splice(0, this._batchSize);
    // not sure what's needed here, need to look at the api.
    // const batchStr = JSON.stringify(batch)

    
    // who knows what we need here!?
    // TODO: enable compression as well
    const messageProtocol = {
      content,
      id: uuid.v4()
    }
    const topicConf = {
      topicName: 'transfer-batch-prepare'
    }
    // Catch async errors explicitly so we don't accidentally miss them
    this._producer.produceMessage(messageProtocol, topicConf)
    .catch(err => {
      throw new Error('Unhandled error sending batch to kafka')
    })

    
    // let messageProtocol = dto.prepareMessageDto({ headers, dataUri, payload, logPrefix, context, isIsoMode })
    //     messageProtocol = await span.injectContextToMessage(messageProtocol)
    //     const { topicConfig, kafkaConfig } = dto.producerConfigDto(Action.TRANSFER, Action.PREPARE, logPrefix)
    
    //     await Kafka.Producer.produceMessage(messageProtocol, topicConfig, kafkaConfig)

  }

}

module.exports = MessageBatcher