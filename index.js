'use strict';
var _ = require('highland'),
    ld = require('lodash');

function returnMultiple(dataStream) {
  const wrapper = _();
  const pipelineFunctions = Array.prototype.slice.call(arguments, 1);
  let resultPipeline = _.pipeline.apply(_, pipelineFunctions);
  if (!_.isStream(dataStream)) {
    // TODO (2016-02-21 Jonathan) Proper idiomatic highland error handling here
    throw Error('dataStream is not a stream');
  }

  _(dataStream).through(resultPipeline).pipe(wrapper);
  return wrapper;
}
module.exports = {
  'processMultiple': returnMultiple,
  'processSingle': function returnSingle(dataStream, options, cb) {

    // If we have options passed in, this is where the variable args start
    let pipelineArgStart = 3;
    if (ld.isFunction(options)) {
      // Options not passed in, arg 1 was the callback function.  Adjust accordingly
      pipelineArgStart = 2;
    }

    const realCb = pipelineArgStart === 3 ? cb : options;
    let realOptions = pipelineArgStart === 3 ? options : {};
    // Force boolean.  If it was truthy before (present and non-falsey), then true, else if
    // not there or falsey, false.
    realOptions.allowFirstFromMultiple = realOptions.allowFirstFromMultiple ? true : false;

    // Build up the arguments for returnMultiple
    // Grab off all the variable arguments that are the processing pipeline
    let callMultipleArgs = Array.prototype.slice.call(arguments, pipelineArgStart);
    // Push the datastream onto the front
    callMultipleArgs.unshift(dataStream);

    // Call returnMultiple and process the data [datum] that came back
    let error = null;
    let resultStream = returnMultiple.apply(returnMultiple,
        Array.prototype.slice.call(callMultipleArgs)).reduce({}, (previous, current) => {
      let result;
      if (ld.isEmpty(previous)) {
        result = current;
      }
      else if(!realOptions.allowFirstFromMultiple) {
       result = Error('processSingle expected a single record from stream');
      }
      else {
        result = previous;
      }
      return result;
    });

    resultStream.pull((err, datum) => {
      if (err) {
        realCb(err, null);
        return;
      }

      if (ld.isError(datum)) {
        err = datum;
        datum = null;
      }

      realCb(err, datum);
    });
  }

};

