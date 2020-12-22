// +build linux

package zfs

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type poolInfo struct {
	name       string
	ioFilename string
}

func (z *Zfs) gatherDatasetStats(acc telegraf.Accumulator) (string, error) {
	properties := []string{"name", "avail", "used", "usedsnap", "usedds"}

	lines, err := z.zdataset(properties)
	if err != nil {
		return "", err
	}

	datasets := []string{}
	for _, line := range lines {
		col := strings.Split(line, "\t")

		datasets = append(datasets, col[0])
	}

	if z.DatasetMetrics {
		for _, line := range lines {
			col := strings.Split(line, "\t")
			if len(col) != len(properties) {
				z.Log.Warnf("Invalid number of columns for line: %s", line)
				continue
			}

			tags := map[string]string{"dataset": col[0]}
			fields := map[string]interface{}{}

			for i, key := range properties[1:] {
				value, err := strconv.ParseInt(col[i+1], 10, 64)
				if err != nil {
					return "", fmt.Errorf("Error parsing %s %q: %s", key, col[i+1], err)
				}
				fields[key] = value
			}

			acc.AddFields("zfs_dataset", fields, tags)
		}
	}

	return strings.Join(datasets, "::"), nil
}

func getPools(kstatPath string) []poolInfo {
	pools := make([]poolInfo, 0)
	poolsDirs, _ := filepath.Glob(kstatPath + "/*/io")

	for _, poolDir := range poolsDirs {
		poolDirSplit := strings.Split(poolDir, "/")
		pool := poolDirSplit[len(poolDirSplit)-2]
		pools = append(pools, poolInfo{name: pool, ioFilename: poolDir})
	}

	return pools
}

func getTags(pools []poolInfo) map[string]string {
	var poolNames string

	for _, pool := range pools {
		if len(poolNames) != 0 {
			poolNames += "::"
		}
		poolNames += pool.name
	}

	return map[string]string{"pools": poolNames}
}

func gatherPoolStats(pool poolInfo, acc telegraf.Accumulator) error {
	lines, err := internal.ReadLines(pool.ioFilename)
	if err != nil {
		return err
	}

	if len(lines) != 3 {
		return err
	}

	keys := strings.Fields(lines[1])
	values := strings.Fields(lines[2])

	keyCount := len(keys)

	if keyCount != len(values) {
		return fmt.Errorf("Key and value count don't match Keys:%v Values:%v", keys, values)
	}

	tag := map[string]string{"pool": pool.name}
	fields := make(map[string]interface{})
	for i := 0; i < keyCount; i++ {
		value, err := strconv.ParseInt(values[i], 10, 64)
		if err != nil {
			return err
		}
		fields[keys[i]] = value
	}
	acc.AddFields("zfs_pool", fields, tag)

	return nil
}

func run(command string, args ...string) ([]string, error) {
	cmd := exec.Command(command, args...)
	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf
	err := cmd.Run()

	stdout := strings.TrimSpace(outbuf.String())
	stderr := strings.TrimSpace(errbuf.String())

	if _, ok := err.(*exec.ExitError); ok {
		return nil, fmt.Errorf("%s error: %s", command, stderr)
	}
	return strings.Split(stdout, "\n"), nil
}

func (z *Zfs) Gather(acc telegraf.Accumulator) error {
	kstatMetrics := z.KstatMetrics
	if len(kstatMetrics) == 0 {
		// vdev_cache_stats is deprecated
		// xuio_stats are ignored because as of Sep-2016, no known
		// consumers of xuio exist on Linux
		kstatMetrics = []string{"abdstats", "arcstats", "dnodestats", "dbufcachestats",
			"dmu_tx", "fm", "vdev_mirror_stats", "zfetchstats", "zil"}
	}

	kstatPath := z.KstatPath
	if len(kstatPath) == 0 {
		kstatPath = "/proc/spl/kstat/zfs"
	}

	pools := getPools(kstatPath)
	tags := getTags(pools)

	if z.PoolMetrics {
		for _, pool := range pools {
			err := gatherPoolStats(pool, acc)
			if err != nil {
				return err
			}
		}
	}
	datasetNames, err := z.gatherDatasetStats(acc)
	if err != nil {
		return err
	}
	tags["datasets"] = datasetNames

	fields := make(map[string]interface{})
	for _, metric := range kstatMetrics {
		lines, err := internal.ReadLines(kstatPath + "/" + metric)
		if err != nil {
			continue
		}
		for i, line := range lines {
			if i == 0 || i == 1 {
				continue
			}
			if len(line) < 1 {
				continue
			}
			rawData := strings.Split(line, " ")
			key := metric + "_" + rawData[0]
			if metric == "zil" || metric == "dmu_tx" || metric == "dnodestats" {
				key = rawData[0]
			}
			rawValue := rawData[len(rawData)-1]
			value, _ := strconv.ParseInt(rawValue, 10, 64)
			fields[key] = value
		}
	}
	acc.AddFields("zfs", fields, tags)
	return nil
}

func zdataset(properties []string) ([]string, error) {
	return run("zfs", []string{"list", "-Hp", "-o", strings.Join(properties, ",")}...)
}

func init() {
	inputs.Add("zfs", func() telegraf.Input {
		return &Zfs{zdataset: zdataset}
	})
}
