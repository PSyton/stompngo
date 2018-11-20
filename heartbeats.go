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
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

/*
	Initialize heart beats if necessary and possible.

	Return an error, possibly nil, to mainline if initialization can not
	complete.  Establish heartbeat send and receive goroutines as necessary.
*/
func (c *Connection) initializeHeartBeats(ch Headers) (e error) {
	// Client wants Heartbeats ?
	vc, ok := ch.Contains(HK_HEART_BEAT)
	if !ok || vc == "0,0" {
		return nil
	}
	// Server wants Heartbeats ?
	vs, ok := c.ConnectResponse.Headers.Contains(HK_HEART_BEAT)
	if !ok || vs == "0,0" {
		return nil
	}
	// Work area, may or may not become connection heartbeat data
	c.hbd = heartBeatData{cx: 0, cy: 0, sx: 0, sy: 0,
		hbs: true, hbr: true, // possible reset later
		sti: 0, rti: 0,
		shutdown:  make(chan struct{}),
		sendCount: 0, receiveCount: 0,
		shutdownFlag: 0}

	// Client specified values
	cp := strings.Split(vc, ",")
	if len(cp) != 2 { // S/B caught by the server first
		return Error("invalid client heart-beat header: " + vc)
	}
	c.hbd.cx, e = strconv.ParseInt(cp[0], 10, 64)
	if e != nil {
		return Error("non-numeric cx heartbeat value: " + cp[0])
	}
	c.hbd.cy, e = strconv.ParseInt(cp[1], 10, 64)
	if e != nil {
		return Error("non-numeric cy heartbeat value: " + cp[1])
	}

	// Server specified values
	sp := strings.Split(vs, ",")
	if len(sp) != 2 {
		return Error("invalid server heart-beat header: " + vs)
	}
	c.hbd.sx, e = strconv.ParseInt(sp[0], 10, 64)
	if e != nil {
		return Error("non-numeric sx heartbeat value: " + sp[0])
	}
	c.hbd.sy, e = strconv.ParseInt(sp[1], 10, 64)
	if e != nil {
		return Error("non-numeric sy heartbeat value: " + sp[1])
	}

	// Check for sending needed
	c.hbd.hbs = (c.hbd.cx > 0 && c.hbd.sy > 0)

	// Check for receiving needed
	c.hbd.hbr = (c.hbd.sx > 0 && c.hbd.cy > 0)

	// ========================================================================
	if !c.hbd.hbs && !c.hbd.hbr {
		return nil // none required
	}

	// ========================================================================

	if c.hbd.hbs { // Finish sender parameters if required
		sm := max(c.hbd.cx, c.hbd.sy) // ticker interval, ms
		c.hbd.sti = 1000000 * sm      // ticker interval, ns
		c.updateSendTime()
		// fmt.Println("start send ticker")
		go c.sendTicker()
	}

	if c.hbd.hbr { // Finish receiver parameters if required
		rm := max(c.hbd.sx, c.hbd.cy) // ticker interval, ms
		c.hbd.rti = 1000000 * rm      // ticker interval, ns
		c.updateReceiveTime()
		// fmt.Println("start receive ticker")
		go c.receiveTicker()
	}

	return nil
}

/*
	The heart beat send ticker.
*/
func (c *Connection) sendTicker() {
	interval := c.hbd.sti

	for {
		select {
		case curTime := <-time.After(time.Duration(interval)):
			lastSend := atomic.LoadInt64(&c.hbd.lastSendTime)
			diff := curTime.UnixNano() - lastSend

			c.log("HeartBeat Send TIC", "TickerVal", curTime.UnixNano(),
				"LastSend", lastSend, "diff", diff/1000000)

			if diff < c.hbd.sti {
				interval = c.hbd.sti - diff
				continue
			}
			c.log("HeartBeat Send data")
			interval = c.hbd.sti
			// Send a heartbeat
			f := Frame{"\n", Headers{}, NULLBUFF} // Heartbeat frame
			r := make(chan error)
			if e := c.writeWireData(wiredata{f, r}); e != nil {
				return
			}
			e := <-r
			//
			if e == nil {
				atomic.AddInt64(&c.hbd.sendCount, 1)
			} else {
				c.log("Heartbeat Send Failure: ", e, "")
				c.handleWireError(e)
				return
			}
		case <-c.hbd.shutdown:
			c.log("Heartbeat Send Shutdown from 'shutdown'", time.Now())
			return
		case <-c.ssdc:
			c.log("Heartbeat Send Shutdown from 'ssdc'", time.Now())
			return
		} // End of select
	} // End of for
}

/*
	The heart beat receive ticker.
*/
func (c *Connection) receiveTicker() {
	for {
		select {
		case ct := <-time.After(time.Duration(c.hbd.rti)):
			flr := atomic.LoadInt64(&c.hbd.lastReceiveTime)
			ld := ct.UnixNano() - flr
			c.log("HeartBeat Receive TIC", "TickerVal", ct.UnixNano(),
				"LastReceive", flr, "Diff", ld/1000000)
			if ld > (c.hbd.rti + (c.hbd.rti / 2)) { // swag plus to be tolerant
				c.log("HeartBeat Receive Read is dirty")
				if ld > c.hbd.rti*2 {
					c.log("HeartBeat - connection stollen")
					c.handleWireError(ECONNSTOLEN)
					return
				}
			} else {
				atomic.AddInt64(&c.hbd.receiveCount, 1)
			}
		case <-c.hbd.shutdown:
			c.log("Heartbeat Receive Shutdown", time.Now())
			return
		} // End of select
	} // End of for
}
