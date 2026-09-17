// Package stratum implements the newline-delimited JSON-RPC dialect used by
// SiaMining Stratum servers.
package stratum

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	callTimeout     = 30 * time.Second
	maxMessageBytes = 16 << 20
)

type request struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     uint64   `json:"id"`
}

type response struct {
	ID     uint64          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type callResult struct {
	value interface{}
	err   error
}

// ErrorCallback is called once when an established connection fails.
type ErrorCallback func(error)

// NotificationHandler handles the decoded parameter array of a notification.
type NotificationHandler func([]interface{})

// Client owns one Stratum TCP connection. It is not reusable after Close.
type Client struct {
	socket net.Conn

	writeMu sync.Mutex
	seqMu   sync.Mutex
	seq     uint64

	callsMu sync.Mutex
	pending map[uint64]chan callResult

	handlersMu           sync.RWMutex
	notificationHandlers map[string]NotificationHandler

	closeOnce sync.Once
	errMu     sync.RWMutex
	done      chan struct{}
	closeErr  error

	ErrorCallback ErrorCallback
}

// Dial connects to host and starts the response reader.
func (c *Client) Dial(host string) error {
	conn, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		return err
	}
	c.socket = conn
	c.pending = make(map[uint64]chan callResult)
	c.done = make(chan struct{})
	go c.listen()
	return nil
}

// Close releases the connection and wakes all pending calls.
func (c *Client) Close() {
	c.fail(net.ErrClosed)
}

// Done is closed when the connection stops.
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// Err returns the error that stopped the connection.
func (c *Client) Err() error {
	c.errMu.RLock()
	defer c.errMu.RUnlock()
	return c.closeErr
}

// SetNotificationHandler registers a handler before Dial is called.
func (c *Client) SetNotificationHandler(method string, handler NotificationHandler) {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()
	if c.notificationHandlers == nil {
		c.notificationHandlers = make(map[string]NotificationHandler)
	}
	c.notificationHandlers[method] = handler
}

func (c *Client) nextID() uint64 {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	c.seq++
	return c.seq
}

func (c *Client) listen() {
	scanner := bufio.NewScanner(c.socket)
	scanner.Buffer(make([]byte, 64<<10), maxMessageBytes)
	for scanner.Scan() {
		raw := scanner.Bytes()
		var message response
		if err := json.Unmarshal(raw, &message); err != nil {
			c.fail(fmt.Errorf("decode Stratum message: %w", err))
			return
		}
		if message.Method != "" {
			c.dispatchNotification(message)
			continue
		}
		c.dispatchResponse(message)
	}
	if err := scanner.Err(); err != nil {
		c.fail(fmt.Errorf("read Stratum message: %w", err))
	} else {
		c.fail(errors.New("Stratum server closed the connection"))
	}
}

func (c *Client) dispatchNotification(message response) {
	var params []interface{}
	if len(message.Params) != 0 && string(message.Params) != "null" {
		if err := json.Unmarshal(message.Params, &params); err != nil {
			c.fail(fmt.Errorf("decode %s notification: %w", message.Method, err))
			return
		}
	}
	c.handlersMu.RLock()
	handler := c.notificationHandlers[message.Method]
	c.handlersMu.RUnlock()
	if handler != nil {
		handler(params)
	}
}

func (c *Client) dispatchResponse(message response) {
	c.callsMu.Lock()
	call := c.pending[message.ID]
	delete(c.pending, message.ID)
	c.callsMu.Unlock()
	if call == nil {
		return
	}
	if err := decodeRPCError(message.Error); err != nil {
		call <- callResult{err: err}
		return
	}
	var result interface{}
	if len(message.Result) != 0 {
		if err := json.Unmarshal(message.Result, &result); err != nil {
			call <- callResult{err: fmt.Errorf("decode Stratum result: %w", err)}
			return
		}
	}
	call <- callResult{value: result}
}

func decodeRPCError(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var fields []interface{}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("Stratum error: %s", raw)
	}
	if len(fields) >= 2 {
		if message, ok := fields[1].(string); ok && message != "" {
			return errors.New(message)
		}
	}
	return fmt.Errorf("Stratum error: %s", raw)
}

func (c *Client) fail(err error) {
	c.closeOnce.Do(func() {
		c.errMu.Lock()
		c.closeErr = err
		c.errMu.Unlock()
		if c.socket != nil {
			_ = c.socket.Close()
		}
		if c.done != nil {
			close(c.done)
		}

		c.callsMu.Lock()
		pending := c.pending
		c.pending = make(map[uint64]chan callResult)
		c.callsMu.Unlock()
		for _, call := range pending {
			call <- callResult{err: err}
		}

		if c.ErrorCallback != nil && !errors.Is(err, net.ErrClosed) {
			go c.ErrorCallback(err)
		}
	})
}

// Call invokes a Stratum method and waits up to 30 seconds for its response.
func (c *Client) Call(serviceMethod string, args []string) (interface{}, error) {
	if c.socket == nil || c.done == nil {
		return nil, errors.New("Stratum client is not connected")
	}
	id := c.nextID()
	call := make(chan callResult, 1)
	c.callsMu.Lock()
	c.pending[id] = call
	c.callsMu.Unlock()
	defer func() {
		c.callsMu.Lock()
		delete(c.pending, id)
		c.callsMu.Unlock()
	}()

	payload, err := json.Marshal(request{Method: serviceMethod, Params: args, ID: id})
	if err != nil {
		return nil, err
	}
	payload = append(payload, '\n')
	c.writeMu.Lock()
	_ = c.socket.SetWriteDeadline(time.Now().Add(callTimeout))
	_, err = c.socket.Write(payload)
	_ = c.socket.SetWriteDeadline(time.Time{})
	c.writeMu.Unlock()
	if err != nil {
		c.fail(err)
		return nil, err
	}

	timer := time.NewTimer(callTimeout)
	defer timer.Stop()
	select {
	case result := <-call:
		return result.value, result.err
	case <-timer.C:
		return nil, fmt.Errorf("%s timed out after %s", serviceMethod, callTimeout)
	case <-c.done:
		if err := c.Err(); err != nil {
			return nil, err
		}
		return nil, net.ErrClosed
	}
}
