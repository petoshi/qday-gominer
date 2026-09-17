package sia

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"reflect"
	"sync"
	"time"

	"github.com/petoshi/qday-gominer/clients"
	"github.com/petoshi/qday-gominer/clients/stratum"
	"golang.org/x/crypto/blake2b"
)

const (
	//HashSize is the length of a sia hash
	HashSize = 32
)

// Target declares what a solution should be smaller than to be accepted
type Target [HashSize]byte

type stratumJob struct {
	JobID        string
	PrevHash     []byte
	Coinbase1    []byte
	Coinbase2    []byte
	MerkleBranch [][]byte
	Version      string
	NBits        string
	NTime        []byte
	CleanJobs    bool
	ExtraNonce2  stratum.ExtraNonce2
}

// StratumClient is a sia client using the stratum protocol
type StratumClient struct {
	connectionstring string
	User             string
	Password         string

	mutex           sync.Mutex // protects following
	stratumclient   *stratum.Client
	extranonce1     []byte
	extranonce2Size uint
	target          Target
	currentJob      stratumJob
	startOnce       sync.Once
	clients.BaseClient
}

// Start connects to the Stratum server and keeps reconnecting until the
// process exits.
func (sc *StratumClient) Start() {
	sc.startOnce.Do(func() { go sc.connectLoop() })
}

func (sc *StratumClient) connectLoop() {
	delay := time.Second
	for {
		if err := sc.connect(); err != nil {
			log.Println("Stratum connection ended:", err)
		}
		time.Sleep(delay)
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}

func (sc *StratumClient) connect() error {
	client := &stratum.Client{}
	sc.subscribeToStratumDifficultyChanges(client)
	sc.subscribeToStratumJobNotifications(client)

	log.Println("Connecting to", sc.connectionstring)
	if err := client.Dial(sc.connectionstring); err != nil {
		return err
	}

	sc.mutex.Lock()
	sc.DeprecateOutstandingJobs()
	sc.currentJob = stratumJob{}
	sc.stratumclient = client
	sc.mutex.Unlock()

	result, err := client.Call("mining.subscribe", []string{"qday-gominer"})
	if err != nil {
		client.Close()
		return fmt.Errorf("subscribe: %w", err)
	}
	reply, ok := result.([]interface{})
	if !ok || len(reply) < 3 {
		client.Close()
		return fmt.Errorf("invalid subscribe response: %v", result)
	}
	extranonce1, err := stratum.HexStringToBytes(reply[1])
	if err != nil {
		client.Close()
		return errors.New("invalid extranonce1 in subscribe response")
	}
	extranonce2Size, ok := reply[2].(float64)
	if !ok {
		client.Close()
		return fmt.Errorf("invalid extranonce2 size %v (%v)", reply[2], reflect.TypeOf(reply[2]))
	}
	if err := validateQDAYV1Extranonces(extranonce1, extranonce2Size); err != nil {
		client.Close()
		return err
	}
	sc.mutex.Lock()
	sc.extranonce1 = extranonce1
	sc.extranonce2Size = uint(extranonce2Size)
	sc.mutex.Unlock()

	result, err = client.Call("mining.authorize", []string{sc.User, sc.Password})
	if err != nil {
		client.Close()
		return fmt.Errorf("authorize: %w", err)
	}
	authorized, ok := result.(bool)
	if !ok || !authorized {
		client.Close()
		return fmt.Errorf("worker authorization rejected: %v", result)
	}
	log.Println("Authorized", sc.User)

	<-client.Done()
	return client.Err()
}

func validateQDAYV1Extranonces(extranonce1 []byte, extranonce2Size float64) error {
	if len(extranonce1) != 4 || extranonce2Size != 4 {
		return errors.New("QDAY v1 mining requires a 4+4 byte extranonce job; mining starts at block 9,100")
	}
	return nil
}

func (sc *StratumClient) subscribeToStratumDifficultyChanges(client *stratum.Client) {
	client.SetNotificationHandler("mining.set_difficulty", func(params []interface{}) {
		if params == nil || len(params) < 1 {
			log.Println("ERROR No difficulty parameter supplied by stratum server")
			return
		}
		diff, ok := params[0].(float64)
		if !ok {
			log.Println("ERROR Invalid difficulty supplied by stratum server:", params[0])
			return
		}
		log.Println("Stratum server changed difficulty to", diff)
		sc.setDifficulty(diff)
	})
}

func (sc *StratumClient) subscribeToStratumJobNotifications(client *stratum.Client) {
	client.SetNotificationHandler("mining.notify", func(params []interface{}) {
		log.Println("New job received from stratum server")
		if params == nil || len(params) < 9 {
			log.Println("ERROR Wrong number of parameters supplied by stratum server")
			return
		}

		sj := stratumJob{}
		sc.mutex.Lock()
		sj.ExtraNonce2.Size = sc.extranonce2Size
		sc.mutex.Unlock()

		var ok bool
		var err error
		if sj.JobID, ok = params[0].(string); !ok {
			log.Println("ERROR Wrong job_id parameter supplied by stratum server")
			return
		}
		if sj.PrevHash, err = stratum.HexStringToBytes(params[1]); err != nil {
			log.Println("ERROR Wrong prevhash parameter supplied by stratum server")
			return
		} else if len(sj.PrevHash) != 32 {
			log.Println("ERROR prevhash must be 32 bytes")
			return
		}
		if sj.Coinbase1, err = stratum.HexStringToBytes(params[2]); err != nil {
			log.Println("ERROR Wrong coinb1 parameter supplied by stratum server")
			return
		} else if len(sj.Coinbase1) != 23 {
			log.Println("ERROR QDAY v1 coinb1 must be 23 bytes")
			return
		}
		if sj.Coinbase2, err = stratum.HexStringToBytes(params[3]); err != nil {
			log.Println("ERROR Wrong coinb2 parameter supplied by stratum server")
			return
		} else if len(sj.Coinbase2) != 2 {
			log.Println("ERROR QDAY v1 coinb2 must be 2 bytes")
			return
		}

		//Convert the merklebranch parameter
		merklebranch, ok := params[4].([]interface{})
		if !ok || len(merklebranch) > 64 {
			log.Println("ERROR Wrong merkle_branch parameter supplied by stratum server")
			return
		}
		sj.MerkleBranch = make([][]byte, len(merklebranch), len(merklebranch))
		for i, branch := range merklebranch {
			if sj.MerkleBranch[i], err = stratum.HexStringToBytes(branch); err != nil {
				log.Println("ERROR Wrong merkle_branch parameter supplied by stratum server")
				return
			} else if len(sj.MerkleBranch[i]) != 32 {
				log.Println("ERROR merkle branch values must be 32 bytes")
				return
			}
		}

		if sj.Version, ok = params[5].(string); !ok {
			log.Println("ERROR Wrong version parameter supplied by stratum server")
			return
		}
		if sj.NBits, ok = params[6].(string); !ok {
			log.Println("ERROR Wrong nbits parameter supplied by stratum server")
			return
		}
		if sj.NTime, err = stratum.HexStringToBytes(params[7]); err != nil {
			log.Println("ERROR Wrong ntime parameter supplied by stratum server")
			return
		} else if len(sj.NTime) != 8 {
			log.Println("ERROR ntime must be 8 bytes")
			return
		}
		if sj.CleanJobs, ok = params[8].(bool); !ok {
			log.Println("ERROR Wrong clean_jobs parameter supplied by stratum server")
			return
		}
		sc.addNewStratumJob(sj)
	})
}

func (sc *StratumClient) addNewStratumJob(sj stratumJob) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	sc.currentJob = sj
	if sj.CleanJobs {
		sc.DeprecateOutstandingJobs()
	}
	sc.AddJobToDeprecate(sj.JobID)
}

// IntToTarget converts a big.Int to a Target.
func intToTarget(i *big.Int) (t Target, err error) {
	// Check for negatives.
	if i.Sign() < 0 {
		err = errors.New("Negative target")
		return
	}
	// In the event of overflow, return the maximum.
	if i.BitLen() > 256 {
		err = errors.New("Target is too high")
		return
	}
	b := i.Bytes()
	offset := len(t[:]) - len(b)
	copy(t[offset:], b)
	return
}

func difficultyToTarget(difficulty float64) (target Target, err error) {
	if difficulty <= 0 || math.IsNaN(difficulty) || math.IsInf(difficulty, 0) {
		return target, errors.New("difficulty must be finite and positive")
	}
	diffAsBig := big.NewFloat(difficulty)

	diffOneString := "0x00000000ffff0000000000000000000000000000000000000000000000000000"
	targetOneAsBigInt := &big.Int{}
	if _, ok := targetOneAsBigInt.SetString(diffOneString, 0); !ok {
		return target, errors.New("invalid difficulty-one target")
	}

	targetAsBigFloat := &big.Float{}
	targetAsBigFloat.SetInt(targetOneAsBigInt)
	targetAsBigFloat.Quo(targetAsBigFloat, diffAsBig)
	targetAsBigInt, _ := targetAsBigFloat.Int(nil)
	target, err = intToTarget(targetAsBigInt)
	return
}

func (sc *StratumClient) setDifficulty(difficulty float64) {
	target, err := difficultyToTarget(difficulty)
	if err != nil {
		log.Println("ERROR Error setting difficulty to ", difficulty)
		return
	}
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	sc.target = target
}

// GetHeaderForWork fetches new work from the SIA daemon
func (sc *StratumClient) GetHeaderForWork() (target, header []byte, deprecationChannel chan bool, job interface{}, err error) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	job = sc.currentJob
	if sc.currentJob.JobID == "" {
		err = errors.New("No job received from stratum server yet")
		return
	}

	deprecationChannel = sc.GetDeprecationChannel(sc.currentJob.JobID)

	target = sc.target[:]
	if sc.target == (Target{}) {
		err = errors.New("No valid difficulty received from stratum server yet")
		return
	}

	//Create the arbitrary transaction
	en2 := sc.currentJob.ExtraNonce2.Bytes()
	err = sc.currentJob.ExtraNonce2.Increment()

	arbtx := []byte{0}
	arbtx = append(arbtx, sc.currentJob.Coinbase1...)
	arbtx = append(arbtx, sc.extranonce1...)
	arbtx = append(arbtx, en2...)
	arbtx = append(arbtx, sc.currentJob.Coinbase2...)
	arbtxHash := blake2b.Sum256(arbtx)

	//Construct the merkleroot from the arbitrary transaction and the merklebranches
	merkleRoot := arbtxHash
	for _, h := range sc.currentJob.MerkleBranch {
		m := append([]byte{1}[:], h...)
		m = append(m, merkleRoot[:]...)
		merkleRoot = blake2b.Sum256(m)
	}

	//Construct the header
	header = make([]byte, 0, 80)
	header = append(header, sc.currentJob.PrevHash...)
	header = append(header, []byte{0, 0, 0, 0, 0, 0, 0, 0}[:]...) //empty nonce
	header = append(header, sc.currentJob.NTime...)
	header = append(header, merkleRoot[:]...)

	return
}

// SubmitHeader reports a solution to the stratum server
func (sc *StratumClient) SubmitHeader(header []byte, job interface{}) (err error) {
	if len(header) != 80 {
		return fmt.Errorf("mined header is %d bytes, expected 80", len(header))
	}
	sj, ok := job.(stratumJob)
	if !ok || sj.JobID == "" {
		return errors.New("missing Stratum job for solved header")
	}
	nonce := hex.EncodeToString(header[32:40])
	encodedExtraNonce2 := hex.EncodeToString(sj.ExtraNonce2.Bytes())
	nTime := hex.EncodeToString(sj.NTime)
	sc.mutex.Lock()
	c := sc.stratumclient
	sc.mutex.Unlock()
	if c == nil {
		return errors.New("Stratum client is disconnected")
	}
	result, err := c.Call("mining.submit", []string{sc.User, sj.JobID, encodedExtraNonce2, nTime, nonce})
	if err != nil {
		return
	}
	accepted, ok := result.(bool)
	if !ok || !accepted {
		return fmt.Errorf("share rejected: %v", result)
	}
	log.Println("Share accepted")
	return
}
