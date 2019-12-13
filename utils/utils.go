package utils

import (
	"eth2-exporter/types"
	"fmt"
	"gopkg.in/yaml.v2"
	"html/template"
	"os"
	"os/signal"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Specific to the Prysm testnet
const SecondsPerSlot = 12
const SlotsPerEpoch = 8
const GenesisTimestamp = 1573489682

const PageSize = 500

// Global accessible configs
var Config *types.Config

func GetTemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"formatBlockStatus": FormatBlockStatus,
		"formatValidator":   FormatValidator,
	}
}

func FormatBlockStatus(status uint64) template.HTML {
	if status == 0 {
		return "<span class=\"badge badge-light\">Scheduled</span>"
	} else if status == 1 {
		return "<span class=\"badge badge-success\">Proposed</span>"
	} else if status == 2 {
		return "<span class=\"badge badge-warning\">Missed</span>"
	} else if status == 3 {
		return "<span class=\"badge badge-secondary\">Orphaned</span>"
	} else {
		return "Unknown"
	}
}

func FormatAttestationStatus(status uint64) string {
	if status == 0 {
		return "Scheduled"
	} else if status == 1 {
		return "Attested"
	} else if status == 2 {
		return "Missed"
	} else {
		return "Unknown"
	}
}

func FormatValidator(validator uint64) template.HTML {
	return template.HTML(fmt.Sprintf("<i class=\"fas fa-male\"></i> <a href=\"/validator/%v\">%v</a>", validator, validator))
}

func SlotToTime(slot uint64) time.Time {
	return time.Unix(GenesisTimestamp+int64(slot)*SecondsPerSlot, 0)
}

func TimeToSlot(timestamp uint64) uint64 {
	return (timestamp - GenesisTimestamp) / SecondsPerSlot
}

func EpochToTime(epoch uint64) time.Time {
	return time.Unix(GenesisTimestamp+int64(epoch)*SecondsPerSlot*SlotsPerEpoch, 0)
}

func FormatBalance(balance uint64) string {
	return fmt.Sprintf("%.2f ETH", float64(balance)/float64(1000000000))
}

func WaitForCtrlC() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func ReadConfig(cfg *types.Config, path string) error {
	err := readConfigFile(cfg, path)

	if err != nil {
		return err
	}

	return readConfigEnv(cfg)
}

func readConfigFile(cfg *types.Config, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("Error opening config file %v: %v", path, err)
	}

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(cfg)
	if err != nil {
		return fmt.Errorf("Error decoding config file %v: %v", path, err)
	}

	return nil
}

func readConfigEnv(cfg *types.Config) error {
	return envconfig.Process("", cfg)
}

func FormatPublicKey(publicKey []byte) string {
	return fmt.Sprintf("%x", publicKey)
}
