var sys;
try {
    sys = require( 'util' );
} catch( e ) {
    sys = require( 'sys' );
}
var events = require( 'events' ),
    fs = require( 'fs' );
var Stream = require( 'stream' ).Stream;
var EventEmitter = require( 'events' ).EventEmitter;

var csv = exports;

var Iconv = require( 'iconv' ).Iconv;

function setOptions( obj, options ) {
    options = options || {};
    obj.separator = options.separator || ',';
    obj.quotechar = options.quote || '"';
    obj.escapechar = options.escape || '"';
    obj.commentchar = options.comment || '';
    obj.columnNames = options.columnNames || [];
    obj.columnsFromHeader = options.columnsFromHeader || false;
    obj.nestedQuotes = options.nestedQuotes || false;
    obj.encoding = options.encoding || 'utf8';
    obj.iconv = null;
    if( undefined !== options.encoding && 'utf8' !== options.encoding && 'ascii' !== options.encoding && 'base64' !== options.encoding ) {
        // we will need a converter
        console.log('CONVERTING FROM ' + options.encoding);
        obj.iconv = new Iconv( options.encoding, 'UTF-8' );
    }
}

/**
 * Provides Base CSV Reading capabilities
 * @class CsvReader
 * @extends EventEmitter
 */

/**
 * The constructor
 * @constructor
 * @param LocReadStream {ReadStream} An instance of the LocReadStream Cl
 * @param options {Object} optional paramaters for the reader <br/>
 *     - separator {String}
 *     - quote {String}
 *     - escape {String}
 *     - comment {String}
 *     - columnNames {Boolean}
 *     - columnsFromHeader {Boolean}
 *     - nestedQuotes {Boolean}
 */
var CsvReader = csv.CsvReader = function( LocReadStream, options ) {
    "use strict";

    var myself = this;
    setOptions( myself, options );

    if( this.iconv ) {
        LocReadStream.setEncoding( this.iconv );
    } else {
        // the following call makes LocReadStream return strings
        // however, the converter needs the raw buffers
        // After some investigation: you never get a buffer out of node, with LocReadStream.
        LocReadStream.setEncoding( options.encoding || 'utf8' );
    }

    myself.parsingStatus = {
        rows: 0,
        openRecord: [],
        openField: '',
        lastChar: '',
        quotedField: false,
        commentedLine: false
    };

    if( LocReadStream ) {
        LocReadStream.addListener( 'data', this.parse.bind( this ) );
        LocReadStream.addListener( 'error', this.emit.bind( this, 'error' ) );
        LocReadStream.addListener( 'end', this.end.bind( this ) );

        /**
         * Pauses the LocReadStream
         * @method pause
         * @return {LocReadStream} the LocReadStream instance
         */
        myself.pause = function() {
            LocReadStream.pause();
            return myself;
        };

        /**
         * Resumes the LocReadStream
         * @method resume
         * @return {LocReadStream} the LocReadStream instance
         */
        myself.resume = function() {
            LocReadStream.resume();
            return myself;
        };

        /**
         * Closes the LocReadStream
         * @method destroy
         * @return {LocReadStream} the LocReadStream instance
         */
        myself.destroy = function() {
            LocReadStream.destroy();
            return myself;
        };

        /**
         * Closes the LocReadStream when its file stream has been drained
         * @method destroySoon
         * @return {LocReadStream} the LocReadStream instance
         */
        myself.destroySoon = function() {
            LocReadStream.destroy();
            return myself;
        };
    }

};
sys.inherits( CsvReader, events.EventEmitter );

/**
 * Parses incoming data as a readable CSV file
 * @method parse
 * @param data {Array} Array of values to parse from the incoming file
 */
CsvReader.prototype.parse = function( data ) {
    var ps = this.parsingStatus,
        nextChar;
    if( data ) {
        Object.prototype.toString.call( data );
    }
    // if we have set up a converter, let's go ahead and convert
    if( this.iconv ) {
        data = this.iconv.convert( new Buffer( data ) );
        data = data.toString( 'utf8' );
    }
    if( ps.openRecord.length == 0 ) {
        if( data.charCodeAt( 0 ) === 0xFEFF ) {
            data = data.slice( 1 );
        }
    }
    for( var i = 0; i < data.length; i++ ) {
        var c = data.charAt( i );
        switch( c ) {
            // escape and separator may be the same char, typically '"'
            case this.escapechar:
            case this.quotechar:
                if( ps.commentedLine ) {
                    break;
                }
                var isEscape = false;
                if( c === this.escapechar ) {
                    nextChar = data.charAt( i + 1 );
                    if( this._isEscapable( nextChar ) ) {
                        this._addCharacter( nextChar );
                        i++;
                        isEscape = true;
                    }
                }
                if( !isEscape && (c === this.quotechar) ) {
                    if( ps.openField && !ps.quotedField ) {
                        ps.quotedField = true;
                        break;
                    }
                    if( ps.quotedField ) {
                        // closing quote should be followed by separator unless the nested quotes option is set
                        nextChar = data.charAt( i + 1 );
                        if( nextChar && nextChar != '\r' && nextChar != '\n' && nextChar !== this.separator && this.nestedQuotes != true ) {
                            throw new Error( "separator expected after a closing quote; found " + nextChar );
                        } else {
                            ps.quotedField = false;
                        }
                    } else if( ps.openField === '' ) {
                        ps.quotedField = true;
                    }
                }
                break;
            case this.separator:
                if( ps.commentedLine ) {
                    break;
                }
                if( ps.quotedField ) {
                    this._addCharacter( c );
                } else {
                    this._addField();
                }
                break;
            case '\n':
                // handle CRLF sequence
                if( !ps.quotedField && (ps.lastChar === '\r') ) {
                    break;
                }
            case '\r':
                if( ps.commentedLine ) {
                    ps.commentedLine = false;
                } else if( ps.quotedField ) {
                    this._addCharacter( c );
                } else {
                    this._addField();
                    this._addRecord();
                }
                break;
            case this.commentchar:
                if( ps.commentedLine ) {
                    break;
                }
                if( ps.openRecord.length === 0 && ps.openField === '' && !ps.quotedField ) {
                    ps.commentedLine = true;
                } else {
                    this._addCharacter( c );
                }
            default:
                if( ps.commentedLine ) {
                    break;
                }
                this._addCharacter( c );
        }
        ps.lastChar = c;
    }
};

CsvReader.prototype.end = function() {
    var ps = this.parsingStatus;
    if( ps.quotedField ) {
        this.emit( 'error', new Error( 'Input stream ended but closing quotes expected' ) );
    } else {
        // dump open record
        if( ps.openField ) {
            this._addField();
        }
        if( ps.openRecord.length > 0 ) {
            this._addRecord();
        }
        this.emit( 'end' );
    }
}
CsvReader.prototype._isEscapable = function( c ) {
    if( (c === this.escapechar) || (c === this.quotechar) ) {
        return true;
    }
    return false;
};

CsvReader.prototype._addCharacter = function( c ) {
    this.parsingStatus.openField += c;
};

CsvReader.prototype._addField = function() {
    var ps = this.parsingStatus;
    ps.openRecord.push( ps.openField );
    ps.openField = '';
    ps.quotedField = false;
};

CsvReader.prototype.setColumnNames = function( names ) {
    this.columnNames = names;
};

CsvReader.prototype._addRecord = function() {
    var ps = this.parsingStatus;
    if( this.columnsFromHeader && ps.rows === 0 ) {
        this.setColumnNames( ps.openRecord );
    } else if( this.columnNames != null && this.columnNames.length > 0 ) {
        var objResult = {};
        for( var i = 0; i < this.columnNames.length; i++ ) {
            objResult[this.columnNames[i]] = ps.openRecord[i];
        }
        this.emit( 'data', objResult );
    } else {
        this.emit( 'data', ps.openRecord );
    }
    ps.rows++;
    ps.openRecord = [];
    ps.openField = '';
    ps.quotedField = false;
};

csv.createCsvFileReader = function( path, options ) {
    options = options || {};
    var locReadStream = csv.createLocReadStream( path, {
        'flags': options.flags || 'r'
    } );
    return new CsvReader( locReadStream, options );
};

csv.createCsvStreamReader = function( LocReadStream, options ) {
    if( options === undefined && typeof LocReadStream === 'object' ) {
        options = LocReadStream;
        LocReadStream = undefined;
    }
    options = options || {};
    return new CsvReader( LocReadStream, options );
};

var CsvWriter = csv.CsvWriter = function( writeStream, options ) {
    var self = this;
    self.writeStream = writeStream;
    options = options || {};
    setOptions( self, options );
    self.encoding = options.encoding || 'utf8';

    if( typeof writeStream.setEncoding === 'function' ) {
        writeStream.setEncoding( self.encoding );
    }

    writeStream.addListener( 'drain', this.emit.bind( this, 'drain' ) );
    writeStream.addListener( 'error', this.emit.bind( this, 'error' ) );
    writeStream.addListener( 'close', this.emit.bind( this, 'close' ) );
};
sys.inherits( CsvWriter, events.EventEmitter );

CsvWriter.prototype.writeRecord = function( rec ) {
    if( !rec ) {
        return;
    } // ignore empty records
    if( !Array.isArray( rec ) ) {
        throw new Error( "CsvWriter.writeRecord only takes an array as an argument" );
    }
    _writeArray( this, rec );
};

function _writeArray( writer, arr ) {
    var out = [];
    for( var i = 0; i < arr.length; i++ ) {
        if( i != 0 ) {
            out.push( writer.separator );
        }
        out.push( writer.quotechar );
        _appendField( out, writer, arr[i] );
        out.push( writer.quotechar );
    }
    out.push( "\r\n" );
    writer.writeStream.write( out.join( '' ), this.encoding );
};

function _appendField( outArr, writer, field ) {
    // Make sure field is a string
    if( typeof(field) !== 'string' ) {
        // We are not interested in outputting "null" or "undefined"
        if( typeof(field) !== 'undefined' && field !== null ) {
            field = String( field );
        } else {
            outArr.push( '' );
            return;
        }
    }

    for( var i = 0; i < field.length; i++ ) {
        if( field.charAt( i ) === writer.quotechar || field.charAt( i ) === writer.escapechar ) {
            outArr.push( writer.escapechar );
        }
        outArr.push( field.charAt( i ) );
    }
};

csv.createCsvFileWriter = function( path, options ) {
    options = options || {'flags': 'w'};
    var writeStream = fs.createWriteStream( path, {
        'flags': options.flags || 'w'
    } );
    return new CsvWriter( writeStream, options );
};

csv.createCsvStreamWriter = function( writeStream, options ) {
    return new CsvWriter( writeStream, options );
};

var pool;
var kPoolSize = 64 * 1024;
var kMinPoolSpace = 32;

function allocNewPool() {
    pool = new Buffer( kPoolSize );
    pool.used = 0;
}

csv.createLocReadStream = function( path, options ) {
    return new LocReadStream( path, options );
};

var LocReadStream = csv.LocReadStream = function( path, options ) {
    if( !(this instanceof LocReadStream) ) {
        return new LocReadStream( path, options );
    }

    Stream.call( this );

    var self = this;

    this.path = path;
    this.fd = null;
    this.readable = true;
    this.paused = false;

    this.flags = 'r';
    this.mode = 438;
    /*=0666*/
    this.bufferSize = 64 * 1024;

    options = options || {};

    // Mixin options into this
    var keys = Object.keys( options );
    for( var index = 0, length = keys.length; index < length; index++ ) {
        var key = keys[index];
        this[key] = options[key];
    }

    if( this.encoding ) {
        this.setEncoding( this.encoding );
    }

    if( this.start !== undefined ) {
        if( 'number' !== typeof this.start ) {
            throw TypeError( 'start must be a Number' );
        }
        if( this.end === undefined ) {
            this.end = Infinity;
        } else if( 'number' !== typeof this.end ) {
            throw TypeError( 'end must be a Number' );
        }

        if( this.start > this.end ) {
            throw new Error( 'start must be <= end' );
        }

        this.pos = this.start;
    }

    if( this.fd !== null ) {
        process.nextTick( function() {
            self._read();
        } );
        return;
    }

    fs.open( this.path, this.flags, this.mode, function( err, fd ) {
        if( err ) {
            self.emit( 'error', err );
            self.readable = false;
            return;
        }

        self.fd = fd;
        self.emit( 'open', fd );
        self._read();
    } );
};
sys.inherits(LocReadStream, Stream);

LocReadStream.prototype.setEncoding = function( encoding ) {
    var buf,
        myconverter;
    if( 'function' === typeof(encoding) ) {
        console.log('FUNCTION');
        this._converter = encoding;
        myconverter = encoding;
        this._decoder = function(dataBuffer) {
            console.log('PRE');
            buf = myconverter.convert(dataBuffer);
            console.log('POST');
            return buf.toString();
        };
    } else if ('string'===typeof(encoding)) {
        // encoding is either 'base64', 'ascii' or 'utf-8'.
        var StringDecoder = require('string_decoder').StringDecoder; // lazy load
        console.log('STRING');
        this._decoder = new StringDecoder(encoding);
    }
    else {
        // pass back raw data
        console.log('OTHER');
        this._decoder = null;

    }
};

LocReadStream.prototype._read = function() {
    var self = this;
    if( !this.readable || this.paused || this.reading ) {
        return;
    }

    this.reading = true;

    if( !pool || pool.length - pool.used < kMinPoolSpace ) {
        // discard the old pool. Can't add to the free list because
        // users might have references to slices on it.
        pool = null;
        allocNewPool();
    }

    // Grab another reference to the pool in the case that while we're in the
    // thread pool another read() finishes up the pool, and allocates a new
    // one.
    var thisPool = pool;
    var toRead = Math.min( pool.length - pool.used, ~~this.bufferSize );
    var start = pool.used;

    if( this.pos !== undefined ) {
        toRead = Math.min( this.end - this.pos + 1, toRead );
    }

    function afterRead( err, bytesRead ) {
        self.reading = false;
        if( err ) {
            self.emit( 'error', err );
            self.readable = false;
            return;
        }

        if( bytesRead === 0 ) {
            self.emit( 'end' );
            self.destroy();
            return;
        }

        var b = thisPool.slice( start, start + bytesRead );

        // Possible optimizition here?
        // Reclaim some bytes if bytesRead < toRead?
        // Would need to ensure that pool === thisPool.

        // do not emit events if the stream is paused
        if( self.paused ) {
            self.buffer = b;
            return;
        }

        // do not emit events anymore after we declared the stream unreadable
        if( !self.readable ) {
            return;
        }

        self._emitData( b );
        self._read();
    }

    fs.read( this.fd, pool, pool.used, toRead, this.pos, afterRead );

    if( this.pos !== undefined ) {
        this.pos += toRead;
    }
    pool.used += toRead;
};

LocReadStream.prototype._emitData = function( d ) {
    if( this._decoder ) {
        var string = this._decoder( d );
        if( string.length ) {
            this.emit( 'data', string );
        }
    } else {
        this.emit( 'data', d );
    }
};

LocReadStream.prototype.destroy = function( cb ) {
    var self = this;

    if( !this.readable ) {
        if( cb ) {
            process.nextTick( function() {
                cb( null );
            } );
        }
        return;
    }
    this.readable = false;

    function close() {
        fs.close( self.fd, function( err ) {
            if( err ) {
                if( cb ) {
                    cb( err );
                }
                self.emit( 'error', err );
                return;
            }

            if( cb ) {
                cb( null );
            }
            self.emit( 'close' );
        } );
    }

    if( this.fd === null ) {
        this.addListener( 'open', close );
    } else {
        close();
    }
};

LocReadStream.prototype.pause = function() {
    this.paused = true;
};

LocReadStream.prototype.resume = function() {
    this.paused = false;

    if( this.buffer ) {
        var buffer = this.buffer;
        this.buffer = null;
        this._emitData( buffer );
    }

    // hasn't opened yet.
    if( null == this.fd ) {
        return;
    }

    this._read();
};

