package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/petoshi/qday-gominer/algorithms/sia"
	"github.com/petoshi/qday-gominer/mining"
	"github.com/robvanmieghem/go-opencl/cl"
)

// Version is the released version string of gominer
var Version = "dev"

var intensity = 28
var devicesTypesForMining = cl.DeviceTypeGPU

func main() {
	log.SetOutput(os.Stdout)
	printVersion := flag.Bool("v", false, "Show version and exit")
	listDevices := flag.Bool("list", false, "List OpenCL devices and exit")
	useCPU := flag.Bool("cpu", false, "If set, also use the CPU for mining, only GPU's are used by default")
	flag.IntVar(&intensity, "I", intensity, "Intensity")
	host := flag.String("url", "stratum+tcp://pool.pqday.com:3333", "QDAY Stratum server URL")
	pooluser := flag.String("user", "", "QDAY payout address and worker name: ADDRESS.WORKER")
	poolpassword := flag.String("password", "x", "stratum password; QDAY pool accepts d=<difficulty>")
	excludedGPUs := flag.String("E", "", "Exclude GPU's: comma separated list of devicenumbers")
	flag.Parse()

	if *printVersion {
		fmt.Println("qday-gominer", Version)
		os.Exit(0)
	}
	if intensity < 1 || intensity > 31 {
		log.Fatalln("Intensity must be between 1 and 31")
	}

	if *useCPU {
		devicesTypesForMining = cl.DeviceTypeAll
	}
	globalItemSize := int(math.Exp2(float64(intensity)))

	platforms, err := cl.GetPlatforms()
	if err != nil {
		log.Fatalf("OpenCL runtime unavailable. Install the GPU vendor's OpenCL driver and try again: %v", err)
	}

	clDevices := make([]*cl.Device, 0, 4)
	for _, platform := range platforms {
		log.Println("Platform", platform.Name())
		platformDevices, err := cl.GetDevices(platform, devicesTypesForMining)
		if err != nil {
			log.Println(err)
		}
		log.Println(len(platformDevices), "device(s) found:")
		for i, device := range platformDevices {
			log.Println(i, "-", device.Type(), "-", device.Name())
			clDevices = append(clDevices, device)
		}
	}

	if len(clDevices) == 0 {
		log.Println("No suitable OpenCL devices found")
		os.Exit(1)
	}
	if *listDevices {
		return
	}
	if strings.HasPrefix(*host, "stratum+tcp://") && strings.TrimSpace(*pooluser) == "" {
		log.Fatalln("Set -user to your QDAY address and worker name: ADDRESS.WORKER")
	}

	//Filter the excluded devices
	miningDevices := make(map[int]*cl.Device)
	for i, device := range clDevices {
		if deviceExcludedForMining(i, *excludedGPUs) {
			continue
		}
		miningDevices[i] = device
	}

	nrOfMiningDevices := len(miningDevices)
	if nrOfMiningDevices == 0 {
		log.Fatalln("No OpenCL devices selected for mining")
	}
	var hashRateReportsChannel = make(chan *mining.HashRateReport, nrOfMiningDevices*10)

	var miner mining.Miner
	log.Println("Starting QDAY mining")
	c := sia.NewClient(*host, *pooluser, *poolpassword)

	miner = &sia.Miner{
		ClDevices:       miningDevices,
		HashRateReports: hashRateReportsChannel,
		Intensity:       intensity,
		GlobalItemSize:  globalItemSize,
		Client:          c,
	}
	miner.Mine()

	// Print a compact device summary without throttling the mining workers on
	// terminal output.
	hashRateReports := make(map[int]float64, nrOfMiningDevices)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case report := <-hashRateReportsChannel:
			hashRateReports[report.MinerID] = report.HashRate
		case <-ticker.C:
			fmt.Print("\r")
			var totalHashRate float64
			for minerID, hashrate := range hashRateReports {
				fmt.Printf("%d-%.1f ", minerID, hashrate)
				totalHashRate += hashrate
			}
			fmt.Printf("Total: %.1f MH/s  ", totalHashRate)
		}
	}
}

// deviceExcludedForMining checks if the device is in the exclusion list
func deviceExcludedForMining(deviceID int, excludedGPUs string) bool {
	excludedGPUList := strings.Split(excludedGPUs, ",")
	for _, excludedGPU := range excludedGPUList {
		if strconv.Itoa(deviceID) == excludedGPU {
			return true
		}
	}
	return false
}
