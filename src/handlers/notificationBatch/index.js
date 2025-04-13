const { logger } = require('../../shared/logger')
const Participant = require('../../domain/participant')
const Config = require('../../lib/config')

const { Kafka: { Consumer, otel }, Util: { Producer } } = require('@mojaloop/central-services-stream')
const { Util, Enum, Util: { Hapi }, Enum: { Tags: { QueryTags } } } = require('@mojaloop/central-services-shared')
const { FspEndpointTypes, FspEndpointTemplates } = Enum.EndPoints
const hubNameRegex = Util.HeaderValidation.getHubNameRegex(Config.HUB_NAME)

const assert = require('assert')
const util = require('util')
const Mustache = require('mustache')
const path = require('path')
const config = require('../../lib/config')


const _validateNotificationsMessage = (message) => {
  try {
    assert(message.value)
    assert(message.value.content)
    assert(message.value.content.count)
    assert(message.value.content.batch)
    assert(message.value.content.metadata)
    assert(
      message.value.content.batch.length,
      message.value.content.metadata.length,
    )
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
  if (error) {
    // need to understand these error conditions
    throw new Error(`Kafka Error: ${error}`)
  }

  assert(messages)
  assert(Array.isArray(messages))
  assert(messages.length === 1, 'Expected only 1 message from Kafka')
  const message = messages[0]

  try {
    // validate the rough shape
    _validateNotificationsMessage(message)
  } catch (err) {
    console.log('TODO: handle invalid message from kafka!')
    return;
  }

  const batch = message.value.content.batch
  const metadata = message.value.content.metadata

  console.log(`LD handleNotifications, handling batch of`, message.value.content.count)
  console.log(`LD handleNotifications, handling batch with id:`, message.value.id)

  const eventType = message.value.metadata.event.type
  const eventAction = message.value.metadata.event.action

  if (eventType !== 'notification') {
    throw new Error(`unsupported metadata.event.type: ${eventType}`)
  }

  switch (eventAction) {
    case 'prepare': return _handleNotificationsPrepare(batch, metadata)
    case 'fulfil': return _handleNotificationsFulfil(batch, metadata)
    default:
      throw new Error(`unsupported metadata.event.action: ${eventAction}`)
  }
}

const _handleNotificationsPrepare = async (batch, metadata) => {
  // TODO: handle errors for prepares that failed

  // now handle forwarding message
  // do a pass to get the dfsp ids to look up endpoints for
  const dfspIds = _getUniqueDfspIds(batch)

  const postTransfersEndpointList = await Promise.all(dfspIds.map(dfspId => {
    return Participant.getEndpoint({
      fsp: dfspId,
      endpointType: FspEndpointTypes.FSPIOP_CALLBACK_URL_TRANSFER_POST
    })
  }))
  assert(dfspIds.length, postTransfersEndpointList.length)
  const postTransfersEndpoints = postTransfersEndpointList.reduce((acc, curr, idx) => {
    const dfspId = dfspIds[idx]
    acc[dfspId] = curr
    return acc
  }, {})

  await Promise.all(batch.map(payload => {
    _forwardPostTransfersNotification(payload, postTransfersEndpoints)
      .catch(err => {
        logger.error(`async error sending transfer: ${util.inspect(err)}`)
      })
  }))
}

const _handleNotificationsFulfil = async (batch, metadata) => {
  const dfspIds = _getUniqueDfspIds(metadata)
  const putTransfersEndpointList = await Promise.all(dfspIds.map(dfspId => {
    return Participant.getEndpoint({
      fsp: dfspId,
      endpointType: FspEndpointTypes.FSPIOP_CALLBACK_URL_TRANSFER_PUT
    })
  }))
  assert(dfspIds.length, putTransfersEndpointList.length)

  const putTransfersEndpoints = putTransfersEndpointList.reduce((acc, curr, idx) => {
    const dfspId = dfspIds[idx]
    acc[dfspId] = curr
    return acc
  }, {})

  // TODO: tigerstyle this
  await Promise.all(batch.map((payload, idx) => {
    const payloadMeta = metadata[idx]
    _forwardPutTransfersNotification(payload, payloadMeta, putTransfersEndpoints)
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

const _forwardPutTransfersNotification = async (payload, metadata, endpointMap) => {
  const endpointTemplate = FspEndpointTemplates.TRANSFERS_PUT
  // might be easier to write this ourselves?
  const headers = {
    "accept": "application/vnd.interoperability.transfers+json;version=1.1",
    "content-type": "application/vnd.interoperability.transfers+json;version=1.1",
    "date": "Sat, 12 Apr 2025 10:12:35 GMT",
    "fspiop-destination": metadata.payerFsp,
    "fspiop-source": metadata.payeeFsp,
    "traceparent": "123",
    "tracestate": "456",
  }

  const baseUrl = endpointMap[metadata.payerFsp];
  assert(baseUrl)

  // TODO: what's the point of all this templating?
  // const uri = Mustache.render(endpointTemplate, { ID: metadata.transferId, fsp: metadata.payerFsp})
  // if (uri.match('{{|}}')) {
  //   throw new Error(` Mustache.render check failed, template rendering must have failed. Input: ${endpointTemplate}. output: ${uri}`)
  // }

  // TODO: I don't know the canonical way to build this url
  // TODO: handle double `//` without breaking `http://`
  const url = `${baseUrl}${metadata.transferId}`

  const response = await Util.Request.sendRequest({
    apiType: Config.API_TYPE,
    url,
    headers,
    source: metadata.payerFsp,
    destination: metadata.payeeFsp,
    method: Enum.Http.RestMethods.PUT,
    payload,
    responseType: Enum.Http.ResponseTypes.JSON,
    // span, 
    // protocolVersions, 
    hubNameRegex
  })
}


/**
 * @function _getUniqueDfspIds
 * @description Given a notification batch, return a list of the DFSPs that need to be contacted
 */
const _getUniqueDfspIds = (batch) => {
  const dfspIdMap = batch.reduce((acc, curr) => {
    assert(curr.payeeFsp)
    assert(curr.payerFsp)
    acc[curr.payeeFsp] = true
    acc[curr.payerFsp] = true
    return acc;
  }, {})

  return Object.keys(dfspIdMap)
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
      "metadata.broker.list": config.DEFAULT_KAFKA_BROKER,
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