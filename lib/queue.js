/*
 * Simplecrawler - queue module
 * https://github.com/cgiffard/node-simplecrawler
 *
 * Copyright (c) 2011-2015, Christopher Giffard
 *
 */

let Redis = require( 'ioredis' ),
    md5 = require( './md5' ),
    Rabbit = require( 'easy-rabbit' ),
    redis, TAIL;

const RABBITMQ_URL = process.env.RABBITMQ_URL;


let NumberComplete = 0;

var allowedStatistics = [
    'requestTime',
    'requestLatency',
    'downloadTime',
    'contentLength',
    'actualDataSize'
];

var FetchQueue = function( rabbitUrl, tailName, redisUrl ) {

    if ( !redis ) {
      redis = new Redis( redisUrl );
    }

    if ( !TAIL ) {
      TAIL = tailName;
    }

    this.waitFn = null;

    this.getItem = function ( fn ) {
        if ( this.queueItem ) {
          console.log( 'queue item' );
          Rabbit.ack( this.queueMsg );
          var item = this.queueItem;
          fn( null, item );
          this.queueItem = null;
          this.queueMsg = null;
        }
        else {
          console.log( 'to wait' );
          this.waitFn = fn;
          return false;
        }
      };


    this.oldestUnfetchedIndex = 0;
    this.completeCache = 0;
    this.scanIndex = {};
  };

module.exports = FetchQueue;

FetchQueue.prototype = [];
FetchQueue.prototype.start = function ( fn ) {
  Rabbit.run( () => {
      Rabbit.connect( RABBITMQ_URL );

      Rabbit.getFrom( TAIL, ( queueItem, msg ) => {

          if ( this.waitFn ) {
            console.log( 'wait fn' );
            this.waitFn( null, queueItem );
            this.waitFn = null;
            Rabbit.ack( msg );
            return;
          }
          console.log( 'rabbit queue item' );
          this.queueMsg = msg;
          this.queueItem = queueItem;
        } );
      fn();
    } );
};

FetchQueue.prototype.add = function( protocol, domain, port, path, depth, callback ) {

    // For legacy reasons
    if ( depth instanceof Function ) {
      callback = depth;
      depth = 1;
    }

    depth = depth || 1;
    callback = callback && callback instanceof Function ? callback : function() {};
    var self = this;

    // Ensure all variables conform to reasonable defaults
    protocol = protocol === 'https' ? 'https' : 'http';

    if ( isNaN( port ) || !port ) {
      return callback( new Error( 'Port must be numeric!' ) );
    }

    var url = protocol + '://' + domain + ( port !== 80 ? ':' + port : '' ) + path;

    self.exists( protocol, domain, port, path,
        function( err, exists ) {
            if ( err ) {
              return callback( err );
            }

            if ( !exists ) {
              var queueItem = {
                  url: url,
                  protocol: protocol,
                  host: domain,
                  port: port,
                  path: path,
                  depth: depth,
                  fetched: false,
                  status: 'queued',
                  stateData: {}
                };

              Rabbit.sendTo( TAIL, queueItem );
              callback( null, queueItem );
            }
            else {
              var error = new Error( 'Resource already exists in queue!' );
              error.code = 'DUP';

              callback( error );
            }
          } );

    // callback( null, queueItem );
  };


// Check if an item already exists in the queue...
FetchQueue.prototype.exists = function( protocol, domain, port, path, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};

    port = port !== 80 ? ':' + port : '';

    var url = ( protocol + '://' + domain + port + path ).toLowerCase(),
        hash = md5( url );

    redis.exists( hash ).then( function ( r ) {
        if ( r === 0 ) {
          redis.set( hash, true );
          callback( null, 0 );
        }
        else {
          callback( null, 1 );
        }
      } );

  };

// Get last item in queue...
FetchQueue.prototype.last = function( callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var item,
        self = this;

    item = self[self.length - 1];
    callback( null, item );
    return item;
  };

// Get item from queue
// TODO: change to distributed queue
FetchQueue.prototype.get = function( id, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var item,
        self = this;

    if ( !isNaN( id ) && self.length > id ) {
      item = self[id];
      callback( null, item );
      return item;
    }
  };

// Get first unfetched item in the queue (and return its index)
FetchQueue.prototype.oldestUnfetchedItem = function( callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    this.getItem( callback );
  };

// Gets the maximum total request time, request latency, or download time
// TODO: change to distributed queue
FetchQueue.prototype.max = function( statisticName, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var maxStatisticValue = 0,
        self = this;

    if ( allowedStatistics.join().indexOf( statisticName ) === - 1 ) {
      // Not a recognised statistic!
      return callback( new Error( 'Invalid statistic.' ) );
    }

    self.forEach( function( item ) {
        if ( item.fetched && item.stateData[statisticName] !== null && item.stateData[statisticName] > maxStatisticValue ) {
          maxStatisticValue = item.stateData[statisticName];
        }
      } );

    callback( null, maxStatisticValue );
    return maxStatisticValue;
  };

// Gets the minimum total request time, request latency, or download time
// TODO: change to distributed queue
FetchQueue.prototype.min = function( statisticName, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var minimum,
        minStatisticValue = Infinity,
        self = this;

    if ( allowedStatistics.join().indexOf( statisticName ) === - 1 ) {
      // Not a recognised statistic!
      return callback( new Error( 'Invalid statistic.' ) );
    }

    self.forEach( function( item ) {
        if ( item.fetched && item.stateData[statisticName] !== null && item.stateData[statisticName] < minStatisticValue ) {
          minStatisticValue = item.stateData[statisticName];
        }
      } );

    minimum = minStatisticValue === Infinity ? 0 : minStatisticValue;
    callback( null, minimum );
    return minimum;
  };

// Gets the minimum total request time, request latency, or download time
// TODO: change to distributed queue
FetchQueue.prototype.avg = function( statisticName, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var average,
        NumberSum = 0,
        NumberCount = 0,
        self = this;

    if ( allowedStatistics.join().indexOf( statisticName ) === - 1 ) {
      // Not a recognised statistic!
      return callback( new Error( 'Invalid statistic.' ) );
    }

    self.forEach( function( item ) {
        if ( item.fetched && item.stateData[statisticName] !== null && !isNaN( item.stateData[statisticName] ) ) {
          NumberSum += item.stateData[statisticName];
          NumberCount++;
        }
      } );
    average = NumberSum / NumberCount;
    callback( null, average );
    return average;
  };

// Gets the number of requests which have been completed.
// TODO: change to distributed queue
FetchQueue.prototype.complete = function( item, body, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    let self = this;

    NumberComplete++;

    item.body = body.toString();
    // TODO: check item content and body content.
    Rabbit.sendTo( TAIL + '_completed', item );

    callback( null, NumberComplete );
    return NumberComplete;
  };

// Gets the number of queue items with the given status
FetchQueue.prototype.countWithStatus = function( status, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var queueItemsMatched = 0,
        self = this;

    self.forEach( function( item ) {
        if ( item.status === status ) {
          queueItemsMatched++;
        }
      } );

    callback( null, queueItemsMatched );
    return queueItemsMatched;
  };

// Gets the number of queue items with the given status
FetchQueue.prototype.getWithStatus = function( status, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var subqueue = [],
        self = this;

    self.forEach( function( item, index ) {
        if ( item.status === status ) {
          subqueue.push( item );
          subqueue[subqueue.length - 1].queueIndex = index;
        }
      } );

    callback( null, subqueue );
    return subqueue;
  };

// Gets the number of requests which have failed for some reason
FetchQueue.prototype.errors = function( callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var total,
        failedCount,
        notFoundCount,
        self = this;

    failedCount = self.countWithStatus( 'failed' );
    notFoundCount = self.countWithStatus( 'notfound' );
    total = failedCount + notFoundCount;
    callback( null, total );
    return total;
  };

// Gets the number of items in the queue
FetchQueue.prototype.getLength = function( callback ) {
    return callback( null, this.length );
  };

// Writes the queue to disk
FetchQueue.prototype.freeze = function( filename, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var self = this;

    // Re-queue in-progress items before freezing...
    self.forEach( function( item ) {
        if ( item.fetched !== true ) {
          item.status = 'queued';
        }
      } );

    fs.writeFile( filename, JSON.stringify( self ), function( err ) {
        callback( err, self );
      } );
  };

// Reads the queue from disk
FetchQueue.prototype.defrost = function( filename, callback ) {
    callback = callback && callback instanceof Function ? callback : function() {};
    var self = this,
        defrostedQueue = [];

    fs.readFile( filename, function( err, fileData ) {
        if ( err ) {
          return callback( err );
        }

        if ( !fileData.toString( 'utf8' ).length ) {
          return callback( new Error( 'Failed to defrost queue from zero-length JSON.' ) );
        }

        try {
          defrostedQueue = JSON.parse( fileData.toString( 'utf8' ) );
        }
        catch ( error ) {
          return callback( error );
        }

        self.oldestUnfetchedIndex = Infinity;
        self.scanIndex = {};

        for ( var index in defrostedQueue ) {
          if ( defrostedQueue.hasOwnProperty( index ) && !isNaN( index ) ) {
            var queueItem = defrostedQueue[index];
            self.push( queueItem );

            if ( queueItem.status !== 'downloaded' ) {
              self.oldestUnfetchedIndex = Math.min(
                      self.oldestUnfetchedIndex, index );
            }

            self.scanIndex[queueItem.url] = true;
          }
        }

        if ( self.oldestUnfetchedIndex === Infinity ) {
          self.oldestUnfetchedIndex = 0;
        }

        callback( null, self );
      } );
  };
