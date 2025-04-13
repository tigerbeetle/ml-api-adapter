const Logger = require('@mojaloop/central-services-logger')

const defaultValue = (maybeValue, dflt) => {
  if (maybeValue === undefined) {
    return dflt
  }

  return maybeValue
}

const PATH_TO_CONFIG_FILE = defaultValue(process.env.PATH_TO_CONFIG_FILE,'../../config/default.json')
Logger.info(`Config - loading config file from '${PATH_TO_CONFIG_FILE}'`)


const RC = require('rc')('MLAPI', require(PATH_TO_CONFIG_FILE))
const fs = require('fs')
const assert = require('assert')


const getFileContent = (path) => {
  if (!fs.existsSync(path)) {
    console.log(`File ${path} doesn't exist, can't enable JWS signing`)
    throw new Error('File doesn\'t exist')
  }
  return fs.readFileSync(path)
}


const stringToBool = (input) => {
  const lowerStr = `${input}`.toLowerCase()
  if (lowerStr === 'false') {
    return false
  }
  if (lowerStr === 'true') {
    return true
  }
  throw new Error(`stringToBool, invalid input: ${input}`)
}

/**
 * @function kafkaWithBrokerOverrides
 * @description Allows us to easily configure the metadata.broker.list without needing to touch
 *   each config file
 */
const kafkaWithBrokerOverrides = (input, defaultBroker) => {
  assert(defaultBroker)
  assert(input.CONSUMER)
  assert(input.PRODUCER)

  Object.keys(input).filter(groupKey => {
    if (groupKey === 'CONSUMER') {
      return true
    }
    if (groupKey === 'PRODUCER') {
      return true
    }
    return false
  }).forEach(groupKey => {
    const group = input[groupKey]

    Object.keys(group).forEach(key => {
      const topic = input[groupKey][key]
      Object.keys(topic).forEach(topicKey => {
        const leafConfig = topic[topicKey]
        const path = `input.${groupKey}.${key}.${topicKey}`
        if (leafConfig.config 
          && leafConfig.config.rdkafkaConf
          && !leafConfig.config.rdkafkaConf['metadata.broker.list']
        ) {
          Logger.info(`Config kafkaWithBrokerOverrides() overriding: ${path}.config.rdkafkaConf['metadata.broker.list']`)
          input[groupKey][key][topicKey]['config']['rdkafkaConf']['metadata.broker.list'] = defaultBroker
        }
      })
    })
  })

  return input
}



const DEFAULT_PROTOCOL_VERSION = {
  CONTENT: {
    DEFAULT: '1.1',
    VALIDATELIST: [
      '1.1',
      '1.0',
      '2.0'
    ]
  },
  ACCEPT: {
    DEFAULT: '1',
    VALIDATELIST: [
      '1',
      '1.0',
      '1.1',
      '2',
      '2.0'
    ]
  }
}

const defaultBroker = defaultValue(RC.KAFKA.DEFAULT_BROKER, 'localhost:9192')
const kafka = kafkaWithBrokerOverrides(RC.KAFKA, defaultBroker)


const getProtocolVersions = (defaultProtocolVersions, overrideProtocolVersions) => {
  const T_PROTOCOL_VERSION = {
    ...defaultProtocolVersions,
    ...overrideProtocolVersions
  }

  if (overrideProtocolVersions && overrideProtocolVersions.CONTENT) {
    T_PROTOCOL_VERSION.CONTENT = {
      ...defaultProtocolVersions.CONTENT,
      ...overrideProtocolVersions.CONTENT
    }
  }
  if (overrideProtocolVersions && overrideProtocolVersions.ACCEPT) {
    T_PROTOCOL_VERSION.ACCEPT = {
      ...defaultProtocolVersions.ACCEPT,
      ...overrideProtocolVersions.ACCEPT
    }
  }

  if (T_PROTOCOL_VERSION.CONTENT &&
    T_PROTOCOL_VERSION.CONTENT.VALIDATELIST &&
    (typeof T_PROTOCOL_VERSION.CONTENT.VALIDATELIST === 'string' ||
      T_PROTOCOL_VERSION.CONTENT.VALIDATELIST instanceof String)) {
    T_PROTOCOL_VERSION.CONTENT.VALIDATELIST = JSON.parse(T_PROTOCOL_VERSION.CONTENT.VALIDATELIST)
  }
  if (T_PROTOCOL_VERSION.ACCEPT &&
    T_PROTOCOL_VERSION.ACCEPT.VALIDATELIST &&
    (typeof T_PROTOCOL_VERSION.ACCEPT.VALIDATELIST === 'string' ||
      T_PROTOCOL_VERSION.ACCEPT.VALIDATELIST instanceof String)) {
    T_PROTOCOL_VERSION.ACCEPT.VALIDATELIST = JSON.parse(T_PROTOCOL_VERSION.ACCEPT.VALIDATELIST)
  }
  return T_PROTOCOL_VERSION
}

// Set config object to be returned
const config = {
  API_TYPE: RC.API_TYPE, // 'fspiop' or 'iso20022'
  IS_ISO_MODE: RC.API_TYPE === 'iso20022',
  PROXY: RC.PROXY_CACHE,
  PAYLOAD_CACHE: RC.PAYLOAD_CACHE,
  ORIGINAL_PAYLOAD_STORAGE: RC.ORIGINAL_PAYLOAD_STORAGE,
  HUB_ID: RC.HUB_PARTICIPANT.ID,
  HUB_NAME: RC.HUB_PARTICIPANT.NAME,
  HOSTNAME: RC.HOSTNAME.replace(/\/$/, ''),
  PORT: RC.PORT,
  AMOUNT: RC.AMOUNT,
  DFSP_URLS: RC.DFSP_URLS,
  SEND_TRANSFER_CONFIRMATION_TO_PAYEE: RC.TRANSFERS.SEND_TRANSFER_CONFIRMATION_TO_PAYEE,
  ERROR_HANDLING: RC.ERROR_HANDLING,
  HANDLERS: RC.HANDLERS,
  HANDLERS_DISABLED: RC.HANDLERS.DISABLED,
  HANDLERS_API: RC.HANDLERS.API,
  HANDLERS_API_DISABLED: RC.HANDLERS.API.DISABLED,
  // TODO (LD): CONFIG here is redundant, this is already config
  // Duplicating to plain KAFKA to maintain backwards compatibility
  KAFKA_CONFIG: kafka,
  KAFKA: {
    /**
     * DEFAULT_BROKER
     * 
     * Overwritten by specific producer/consumer config
     * 
     * Default: localhost:9192
     */
    DEFAULT_BROKER: defaultValue(RC.KAFKA.DEFAULT_BROKER, 'localhost:9192'),

    /**
     * DEBUG_EXTREME_BATCHING
     * 
     * Description: When `true`, uses in-message Kafka batching, where many Prepares and Fulfils
     *   are combined into the same Kafka message.
     * 
     * Default: false
     */
    DEBUG_EXTREME_BATCHING: stringToBool(defaultValue(RC.KAFKA.DEBUG_EXTREME_BATCHING || false)),

  },
  ENDPOINT_CACHE_CONFIG: RC.ENDPOINT_CACHE_CONFIG,
  ENDPOINT_SOURCE_URL: RC.ENDPOINT_SOURCE_URL,
  ENDPOINT_HEALTH_URL: RC.ENDPOINT_HEALTH_URL,
  INSTRUMENTATION_METRICS_DISABLED: RC.INSTRUMENTATION.METRICS.DISABLED,
  INSTRUMENTATION_METRICS_CONFIG: RC.INSTRUMENTATION.METRICS.config,
  ENDPOINT_SECURITY: RC.ENDPOINT_SECURITY,
  ENDPOINT_SECURITY_TLS: RC.ENDPOINT_SECURITY.TLS,
  MAX_FULFIL_TIMEOUT_DURATION_SECONDS: RC.MAX_FULFIL_TIMEOUT_DURATION_SECONDS,
  MAX_CALLBACK_TIME_LAG_DILATION_MILLISECONDS: RC.MAX_CALLBACK_TIME_LAG_DILATION_MILLISECONDS,
  STRIP_UNKNOWN_HEADERS: RC.STRIP_UNKNOWN_HEADERS,
  JWS_SIGN: RC.ENDPOINT_SECURITY.JWS.JWS_SIGN,
  FSPIOP_SOURCE_TO_SIGN: RC.HUB_PARTICIPANT.NAME,
  JWS_SIGNING_KEY_PATH: RC.ENDPOINT_SECURITY.JWS.JWS_SIGNING_KEY_PATH,
  PROTOCOL_VERSIONS: getProtocolVersions(DEFAULT_PROTOCOL_VERSION, RC.PROTOCOL_VERSIONS)
}

if (config.JWS_SIGN) {
  config.JWS_SIGNING_KEY = getFileContent(config.JWS_SIGNING_KEY_PATH)
}

// Validate config


module.exports = config
