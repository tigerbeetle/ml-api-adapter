const { logger } = require('../../shared/logger')
const Participant = require('../../domain/participant')
const Config = require('../../lib/config')

const { Kafka: { Consumer, otel }, Util: { Producer } } = require('@mojaloop/central-services-stream')
const { Util, Enum, Util: { Hapi }, Enum: { Tags: { QueryTags } } } = require('@mojaloop/central-services-shared')
const { FspEndpointTypes, FspEndpointTemplates } = Enum.EndPoints
const hubNameRegex = Util.HeaderValidation.getHubNameRegex(Config.HUB_NAME)

const assert = require('assert')
const util = require('util')


const _validateNotificationsMessage = (message) => {
  try {
    assert(message.value)
    assert(message.value.content)
    assert(message.value.content.count)
    assert(message.value.content.batch)
    assert(message.value.content.failed)
    assert(message.value.metadata)
    assert(message.value.metadata.event)
    assert(message.value.metadata.event.type)
    assert(message.value.metadata.event.action)
    assert(message.value.id)
  } catch (err) {
    throw err;
  }
}

const handleNotifications = async (error, messages) => {
  // TODO: separate out the notification types

  if (error) {
    // need to understand these error conditions
    throw new Error(`Kafka Error: ${error}`)
  }

  assert(messages)
  assert(Array.isArray(messages))
  assert(messages.length === 1, 'Expected only 1 message from Kafka')
  const message = messages[0]

  try {
    _validateNotificationsMessage(message)
  } catch (err) {
    console.log('TODO: handle invalid message from kafka!')
    return;
  }

  console.log(`LD handleNotifications, handling batch of`, message.value.content.count)

  console.log(`LD handleNotifications, handling batch with id:`, message.value.id)
  console.log(`LD handleNotifications, handling batch with content:`, message.value.content)
  console.log(`LD handleNotifications, handling batch metadata:`, message.value.metadata)

  const eventType = message.value.metadata.event.type
  const eventAction = message.value.metadata.event.action

  if (eventType !== 'notification') {
    throw new Error(`unsupported metadata.event.type: ${eventType}`)
  }
  if (eventAction !== 'prepare') {
    throw new Error(`unsupported metadata.event.action: ${eventAction}`)
  }

  // TODO: handle errors for prepares that failed

  const batch = message.value.content.batch

  // now handle forwarding message
  // do a pass to get the dfsp ids to look up endpoints for
  const dfspIdMap = batch.reduce((acc, curr) => {
    acc[curr.payeeFsp] = true
    acc[curr.payerFsp] = true
    return acc;
  }, {})

  const postTransfersEndpointList = await Promise.all(Object.keys(dfspIdMap).map(dfspId => {
    return Participant.getEndpoint({
      fsp: dfspId,
      endpointType: FspEndpointTypes.FSPIOP_CALLBACK_URL_TRANSFER_POST
    })
  }))
  assert(Object.keys(dfspIdMap).length, postTransfersEndpointList.length)
  const postTransfersEndpoints = postTransfersEndpointList.reduce((acc, curr, idx) => {
    const dfspId = Object.keys(dfspIdMap)[idx]
    acc[dfspId] = curr
    return acc
  }, {})

  console.log('postTransfersEndpoints', postTransfersEndpoints)

  await Promise.all(batch.map(payload => {
    _forwardPostTransfersNotification(payload, postTransfersEndpoints)
    .catch(err => {
      logger.error(`async error sending transfer: ${util.inspect(err)}`)
    })
  }))
}

const _forwardPostTransfersNotification = async (payload, endpointMap) => {
  const endpointTemplate = FspEndpointTemplates.TRANSFERS_POST
  // might be easier to write this ourselves?
  const headers = {
    "accept": "application/vnd.interoperability.transfers+json;version=1.1",
    "content-type": "application/vnd.interoperability.transfers+json;version=1.1",
    "date": "Sat, 12 Apr 2025 10:12:35 GMT",
    "fspiop-destination": payload.payeeFsp,
    "fspiop-source": payload.payerFsp,
    "traceparent": "123",
    "tracestate": "456",
  }

  const url = endpointMap[payload.payeeFsp];
  assert(url)

  const response = await Util.Request.sendRequest({ 
    apiType: Config.API_TYPE, 
    url,
    headers, 
    source: payload.payerFsp,
    destination: payload.payeeFsp,
    method: Enum.Http.RestMethods.POST, 
    payload, 
    responseType: Enum.Http.ResponseTypes.JSON,
    // span, 
    // protocolVersions, 
    hubNameRegex 
  })

}


const registerHandlerNotifications = async () => {
  const topicName = `notification-batch`
  // TODO: configure
  const consumerConfig = {
    config: {
      mode: 2,
      batchSize: 1,
      pollFrequency: 10,
      recursiveTimeout: 1,
      messageCharset: "utf8",
      messageAsJSON: true,
      sync: true,
      consumeTimeout: 1
    },
    rdkafkaConf: {
      "client.id": "notification-batch",
      "group.id": "notification-batch",
      "metadata.broker.list": "localhost:9192",
      "socket.keepalive.enable": true,
      "allow.auto.create.topics": true,
      "enable.auto.commit": true,
    },
    topicConf: {
      "auto.offset.reset": "earliest"
    }
  }

  const consumer = new Consumer([topicName], consumerConfig)
  await consumer.connect()
  logger.info(`NotificationBatch::startConsumer - Kafka Consumer connected for topicNames: [${topicName}]`)
  consumer.consume(handleNotifications)
}


module.exports = {
  registerHandlerNotifications
}