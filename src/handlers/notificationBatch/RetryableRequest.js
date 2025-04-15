const { Util } = require('@mojaloop/central-services-shared')
const { isReadable } = require('supertest/lib/test')

const axios = require('axios')

/**
 * @class RetryableRequest
 * @description Enqueues callbacks to DFSPs with a backoff/jitter
 */
class RetryableRequest {
  _maxRetries
  _sleepMs

  constructor (maxRetries, sleepMs) {
    this._maxRetries = maxRetries
    this._sleepMs = sleepMs
  }

  async sendRequest(request, retries) {
    // default to _maxRetries
    if (retries === undefined) {
      retries = this._maxRetries
    }

    if (retries === 0) {
      console.log('sendRequest ran out of retries')
      throw new Error('RetryableRequest.sendRequest - out of retries')
    }

    const options = {
      ...request,
      data: request.payload
    }

    try {
      // Ideally we would use Util.Request.sendRequest, but this logs errors pretty aggresively
      // and doesn't expose any controls
      await axios(options)
    }
    catch (err) {
      if (this._isRetryable(err)) {
        await sleepWithJitter(this._sleepMs)
        return this.sendRequest(request, retries - 1)
      }

      throw err
    }
  } 


  _isRetryable(err) {
    console.log('_isRetryable unknown err.code', err.code)

    switch (err.code) {
      case "ERR_BAD_REQUEST": return false
    }

    return true
  }

}


const randomNormal = (mean = 0, stdDev = 1) => {
  let u = 0, v = 0;
  while (u === 0) u = Math.random(); // Avoid 0
  while (v === 0) v = Math.random();
  return mean + stdDev * Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
}

const sleepWithJitter = async (medianSleepTime) => {
  // medianSleepTime +- 10%
  const stdDev = medianSleepTime * 0.1
  const jitterySleepTime = Math.max(0, randomNormal(medianSleepTime, stdDev))

  return new Promise((resolve, reject) => {
   setTimeout(resolve, jitterySleepTime) 
  })
}

module.exports = RetryableRequest

