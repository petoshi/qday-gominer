# QDAY gominer

OpenCL GPU miner for QDAY. This is the maintained QDAY fork of Rob Van
Mieghem's Sia gominer, updated for current Go releases and QDAY's SiaMining
Stratum protocol.

It connects directly to the public pool, mines the native 80-byte
BLAKE2b-256 work item and supports the QDAY v1 `4+4` extranonce format active
from mainnet block 9,100.

## Run it

Download the latest release, install the OpenCL driver supplied by your GPU
vendor, and list the devices visible to the miner. Official release archives
target Linux x86-64, Linux ARM64 and Windows x86-64. macOS users can build the
same source with a C compiler and the platform OpenCL development files. The
Windows archive needs no separate installer; the GPU vendor's driver supplies
the required `OpenCL.dll`.

```text
qday-gominer -list
```

Start mining. Replace the address and worker name:

```text
qday-gominer -url stratum+tcp://pool.pqday.com:3333 -user YOUR_QDAY_ADDRESS.rig1
```

The password defaults to `x`. To request a fixed share difficulty from the
QDAY pool:

```text
qday-gominer -user YOUR_QDAY_ADDRESS.rig1 -password d=0.01
```

Useful options:

```text
-I 28          OpenCL intensity; valid range 1 through 31
-E 0,2         exclude device numbers 0 and 2
-list          list OpenCL devices and exit
-v             print the version and exit
```

The miner reconnects automatically when the pool changes jobs or restarts.
At the QDAY v1 activation boundary the pool deliberately reconnects every
worker so the extranonce format cannot be mixed across consensus rules.

There is no developer fee in this fork.

## Build

Go 1.26, a C compiler, OpenCL headers and the vendor OpenCL runtime are
required. On Debian or Ubuntu:

```text
sudo apt install build-essential ocl-icd-opencl-dev
go build -o qday-gominer .
```

The build can succeed with the generic OpenCL loader, but mining still needs a
working NVIDIA, AMD or Intel OpenCL driver. `go test ./...` runs the protocol
and known-header tests everywhere; the kernel test also runs when an OpenCL
device is present.

## Protocol checks

The test suite verifies:

- the exact stock Obelisk/Sia 80-byte header vector;
- the QDAY v1 compact mining transaction and `4+4` extranonces;
- difficulty and target conversion;
- authorization, large Stratum messages, share submission and reconnect-safe
  request handling;
- known solved Sia BLAKE2b headers through the OpenCL kernel when hardware is
  available.

Pool: <https://pool.pqday.com>

QDAY: <https://pqday.com>

Source: <https://github.com/petoshi/qday-gominer>

The original gominer copyright and BSD license remain in [LICENSE](LICENSE).
