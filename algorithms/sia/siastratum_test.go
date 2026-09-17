package sia

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDifficultyToTarget(t *testing.T) {
	diff, _ := strconv.ParseFloat("0.99998474121094105", 64)

	expectedTarget := "0x00000000fffffffffffefffeffff00000001000200020000fffefffcfffbfffd"

	target, err := difficultyToTarget(diff)
	if err != nil {
		t.Error(err)
	}

	if expectedTarget != ("0x" + hex.EncodeToString(target[:])) {
		t.Error("0x"+hex.EncodeToString(target[:]), "returned instead of", expectedTarget)
	}
}

func TestRejectsPreActivationExtranonces(t *testing.T) {
	if err := validateQDAYV1Extranonces(nil, 0); err == nil {
		t.Fatal("accepted a pre-activation Stratum subscription")
	}
	if err := validateQDAYV1Extranonces([]byte{1, 2, 3, 4}, 4); err != nil {
		t.Fatal(err)
	}
}

func TestQDAYV1ObeliskWorkVector(t *testing.T) {
	client := &StratumClient{extranonce1: mustHex(t, "01020304"), extranonce2Size: 4}
	client.DeprecateOutstandingJobs()
	for i := range client.target {
		client.target[i] = 0xff
	}
	job := stratumJob{
		JobID:        "qday-v1-vector",
		PrevHash:     mustHex(t, "5100000000000000000000000000000000000000000000000000000000000059"),
		Coinbase1:    mustHex(t, "0200010000000000001000000000000000514441590204"),
		Coinbase2:    mustHex(t, "0000"),
		MerkleBranch: [][]byte{mustHex(t, "aa000000000000000000000000000000000000000000000000000000000000bb")},
		NTime:        mustHex(t, "8877665544332211"),
		CleanJobs:    true,
	}
	job.ExtraNonce2.Size = 4
	job.ExtraNonce2.Value = 0x05060708
	client.addNewStratumJob(job)

	_, header, _, returned, err := client.GetHeaderForWork()
	if err != nil {
		t.Fatal(err)
	}
	const expected = "510000000000000000000000000000000000000000000000000000000000005900000000000000008877665544332211309f36bacf3562bb7950f00159822c3489ebb89d26544d42664ce21144376dc5"
	if got := hex.EncodeToString(header); got != expected {
		t.Fatalf("Sia ASIC header mismatch\n got %s\nwant %s", got, expected)
	}
	returnedJob := returned.(stratumJob)
	if submitted := returnedJob.ExtraNonce2.Bytes(); hex.EncodeToString(submitted) != "05060708" {
		t.Fatalf("wrong submitted extranonce2 %x", submitted)
	}
}

func mustHex(t *testing.T, encoded string) []byte {
	t.Helper()
	value, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestStratumRoundTripWithLargeQDAYJob(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	type rpcMessage struct {
		ID     uint64            `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	serverErr := make(chan error, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		readRequest := func(want string) (rpcMessage, error) {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return rpcMessage{}, err
			}
			var request rpcMessage
			if err := json.Unmarshal(line, &request); err != nil {
				return rpcMessage{}, err
			}
			if request.Method != want {
				return rpcMessage{}, &unexpectedMethodError{got: request.Method, want: want}
			}
			return request, nil
		}
		write := func(value any) error {
			encoded, err := json.Marshal(value)
			if err != nil {
				return err
			}
			encoded = append(encoded, '\n')
			_, err = conn.Write(encoded)
			return err
		}

		subscribe, err := readRequest("mining.subscribe")
		if err != nil {
			serverErr <- err
			return
		}
		if err := write(map[string]any{"id": subscribe.ID, "result": []any{[]any{}, "01020304", 4}, "error": nil}); err != nil {
			serverErr <- err
			return
		}

		authorize, err := readRequest("mining.authorize")
		if err != nil {
			serverErr <- err
			return
		}
		var username, password string
		if len(authorize.Params) != 2 || json.Unmarshal(authorize.Params[0], &username) != nil || json.Unmarshal(authorize.Params[1], &password) != nil {
			serverErr <- &unexpectedCredentialsError{}
			return
		}
		if username != "qday-address.test" || password != "d=.01" {
			serverErr <- &unexpectedCredentialsError{username: username, password: password}
			return
		}
		if err := write(map[string]any{"id": authorize.ID, "result": true, "error": nil}); err != nil {
			serverErr <- err
			return
		}
		if err := write(map[string]any{"id": nil, "method": "mining.set_difficulty", "params": []any{0.01}}); err != nil {
			serverErr <- err
			return
		}

		coinbase1 := "0200010000000000001000000000000000514441590204"
		notify := map[string]any{
			"id":     nil,
			"method": "mining.notify",
			"params": []any{
				"large-qday-job",
				strings.Repeat("01", 32),
				coinbase1,
				"0000",
				[]string{strings.Repeat("02", 32), strings.Repeat("03", 32)},
				"",
				"1d00ffff",
				"0100000000000000",
				true,
				// Extra notification fields are ignored. This one verifies that the
				// JSON-RPC reader is not limited to bufio.Scanner's 64 KiB default.
				strings.Repeat("q", 1<<20),
			},
		}
		if err := write(notify); err != nil {
			serverErr <- err
			return
		}

		submit, err := readRequest("mining.submit")
		if err != nil {
			serverErr <- err
			return
		}
		var submittedUser string
		if len(submit.Params) != 5 || json.Unmarshal(submit.Params[0], &submittedUser) != nil || submittedUser != username {
			serverErr <- &unexpectedCredentialsError{username: submittedUser}
			return
		}
		if err := write(map[string]any{"id": submit.ID, "result": true, "error": nil}); err != nil {
			serverErr <- err
			return
		}
	}()

	client := &StratumClient{
		connectionstring: listener.Addr().String(),
		User:             "qday-address.test",
		Password:         "d=.01",
	}
	connectDone := make(chan error, 1)
	go func() { connectDone <- client.connect() }()

	var target, header []byte
	var job any
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		target, header, _, job, err = client.GetHeaderForWork()
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	} else if len(target) != 32 || len(header) != 80 {
		t.Fatalf("wrong work dimensions: target=%d header=%d", len(target), len(header))
	}
	if err := client.SubmitHeader(header, job); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-serverErr:
		t.Fatal(err)
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Fatal("mock Stratum server did not finish")
	}
	select {
	case <-connectDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Stratum client did not stop after disconnect")
	}
}

type unexpectedMethodError struct{ got, want string }

func (e *unexpectedMethodError) Error() string {
	return "got Stratum method " + e.got + ", want " + e.want
}

type unexpectedCredentialsError struct{ username, password string }

func (e *unexpectedCredentialsError) Error() string {
	return "unexpected Stratum credentials " + e.username + " / " + e.password
}
