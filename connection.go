//
// Copyright © 2011-2017 Guy M. Allard
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package stompngo

import (
	"log"
	"runtime"
	"sync/atomic"
	"time"
)

// Exported Connection methods

/*
	Connected returns the current connection status.
*/
func (c *Connection) Connected() bool {
	return atomic.LoadInt32(&c.connected) != 0
}

func (c *Connection) setConnected(aConnected bool) {
	if aConnected {
		atomic.StoreInt32(&c.connected, 1)
	} else {
		atomic.StoreInt32(&c.connected, 0)
	}
}

/*
	Session returns the broker assigned session id.
*/
func (c *Connection) Session() string {
	return c.session
}

/*
	Protocol returns the current connection protocol level.
*/
func (c *Connection) Protocol() string {
	return c.protocol
}

/*
	SetLogger enables a client defined logger for this connection.

	Set to "nil" to disable logging.

	Example:
		// Start logging
		l := log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)
		c.SetLogger(l)
*/
func (c *Connection) SetLogger(l *log.Logger) {
	logLock.Lock()
	c.logger = l
	logLock.Unlock()
}

/*
	SendTickerInterval returns any heartbeat send ticker interval in ms.  A return
	value of zero means	no heartbeats are being sent.
*/
func (c *Connection) SendTickerInterval() int64 {
	c.hbdLock.Lock()
	defer c.hbdLock.Unlock()

	if c.hbd == nil {
		return 0
	}
	return c.hbd.sti / 1000000
}

/*
	ReceiveTickerInterval returns any heartbeat receive ticker interval in ms.
	A return value of zero means no heartbeats are being received.
*/
func (c *Connection) ReceiveTickerInterval() int64 {
	c.hbdLock.Lock()
	defer c.hbdLock.Unlock()

	if c.hbd == nil {
		return 0
	}
	return c.hbd.rti / 1000000
}

/*
	SendTickerCount returns any heartbeat send ticker count.  A return value of
	zero usually indicates no send heartbeats are enabled.
*/
func (c *Connection) SendTickerCount() int64 {
	c.hbdLock.Lock()
	defer c.hbdLock.Unlock()

	if c.hbd == nil {
		return 0
	}
	return c.hbd.sc
}

/*
	ReceiveTickerCount returns any heartbeat receive ticker count. A return
	value of zero usually indicates no read heartbeats are enabled.
*/
func (c *Connection) ReceiveTickerCount() int64 {
	c.hbdLock.Lock()
	defer c.hbdLock.Unlock()

	if c.hbd == nil {
		return 0
	}
	return c.hbd.rc
}

/*
	FramesRead returns a count of the number of frames read on the connection.
*/
func (c *Connection) FramesRead() int64 {
	return c.mets.tfr
}

/*
	BytesRead returns a count of the number of bytes read on the connection.
*/
func (c *Connection) BytesRead() int64 {
	return c.mets.tbr
}

/*
	FramesWritten returns a count of the number of frames written on the connection.
*/
func (c *Connection) FramesWritten() int64 {
	return c.mets.tfw
}

/*
	BytesWritten returns a count of the number of bytes written on the connection.
*/
func (c *Connection) BytesWritten() int64 {
	return c.mets.tbw
}

/*
	Running returns a time duration since connection start.
*/
func (c *Connection) Running() time.Duration {
	return time.Since(c.mets.st)
}

/*
	SubChanCap returns the current subscribe channel capacity.
*/
func (c *Connection) SubChanCap() int {
	return c.scc
}

/*
	SetSubChanCap sets a new subscribe channel capacity, to be used during future
	SUBSCRIBE operations.
*/
func (c *Connection) SetSubChanCap(nc int) {
	c.scc = nc
	return
}

// Unexported Connection methods

/*
	Log data if possible.
*/
func (c *Connection) log(v ...interface{}) {
	logLock.Lock()
	defer logLock.Unlock()
	if c.logger == nil {
		return
	}
	_, fn, ld, ok := runtime.Caller(1)

	if ok {
		c.logger.Printf("%s %s %d %v\n", c.session, fn, ld, v)
	} else {
		c.logger.Print(c.session, v)
	}
	return
}

/*
	Shutdown heartbeats
*/
func (c *Connection) shutdownHeartBeats() {
	// Shutdown heartbeats if necessary
	c.hbdLock.Lock()
	if c.hbd != nil {
		close(c.hbd.shutdown)
		c.hbd = nil
	}
	c.hbdLock.Unlock()
}

/*
	Shutdown logic.
*/
func (c *Connection) shutdown() {
	c.log("SHUTDOWN", "starts")
	c.shutdownHeartBeats()
	// Close all individual subscribe channels
	// This is a write lock
	c.subsLock.Lock()
	for key := range c.subs {
		close(c.subs[key].messages)
		c.subs[key].cs = true
	}
	c.subsLock.Unlock()
	c.setConnected(false)
	c.log("SHUTDOWN", "ends")
	return
}

/*
	Read error handler.
*/
func (c *Connection) handleWireError(err error) {
	c.log("HDRERR", "starts", err)
	c.setConnected(false)
	c.shutdownHeartBeats() // We are done here

	// Once send error
	if atomic.CompareAndSwapInt32(&c.errorsCount, 0, 1) {
		c.log("HDRERR", "Send error to channel")
		c.errors <- err
		// Try to catch the writer
	}

	c.log("HDRERR", "ends")
	// Let further shutdown logic proceed normally.
	return
}
