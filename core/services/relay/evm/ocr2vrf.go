package evm

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"github.com/smartcontractkit/libocr/gethwrappers2/ocr2aggregator"
	"github.com/smartcontractkit/libocr/offchainreporting2/chains/evmutil"
	"github.com/smartcontractkit/libocr/offchainreporting2/types"
	"github.com/smartcontractkit/sqlx"

	relaytypes "github.com/smartcontractkit/chainlink-relay/pkg/types"
	"github.com/smartcontractkit/chainlink/core/chains/evm"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/services/ocr2/plugins/dkg/config"
)

// DKGProvider provides all components needed for a DKG plugin.
type DKGProvider interface {
	relaytypes.Plugin
}

// OCR2VRFProvider provides all components needed for a OCR2VRF plugin.
type OCR2VRFProvider interface {
	relaytypes.Plugin
}

// OCR2VRFRelayer contains the relayer and instantiating functions for OCR2VRF providers.
type OCR2VRFRelayer interface {
	NewDKGProvider(rargs relaytypes.RelayArgs, pargs relaytypes.PluginArgs) (DKGProvider, error)
	NewOCR2VRFProvider(rargs relaytypes.RelayArgs, pargs relaytypes.PluginArgs) (OCR2VRFProvider, error)
}

var (
	_ OCR2VRFRelayer  = (*ocr2vrfRelayer)(nil)
	_ DKGProvider     = (*dkgProvider)(nil)
	_ OCR2VRFProvider = (*ocr2vrfProvider)(nil)
)

// Relayer with added DKG and OCR2VRF provider functions.
type ocr2vrfRelayer struct {
	db    *sqlx.DB
	chain evm.Chain
	lggr  logger.Logger
}

func NewOCR2VRFRelayer(db *sqlx.DB, chain evm.Chain, lggr logger.Logger) OCR2VRFRelayer {
	return &ocr2vrfRelayer{
		db:    db,
		chain: chain,
		lggr:  lggr,
	}
}

func (r *ocr2vrfRelayer) NewDKGProvider(rargs relaytypes.RelayArgs, pargs relaytypes.PluginArgs) (DKGProvider, error) {
	configWatcher, err := newOCR2VRFConfigProvider(r.lggr, r.chain, rargs.ContractID)
	if err != nil {
		return nil, err
	}
	contractTransmitter, err := newContractTransmitter(r.lggr, rargs, pargs.TransmitterID, configWatcher)
	if err != nil {
		return nil, err
	}

	var pluginConfig config.PluginConfig
	err = json.Unmarshal(pargs.PluginConfig, &pluginConfig)
	if err != nil {
		return nil, err
	}

	return &dkgProvider{
		configWatcher:       configWatcher,
		contractTransmitter: contractTransmitter,
		pluginConfig:        pluginConfig,
	}, nil
}

func (r *ocr2vrfRelayer) NewOCR2VRFProvider(rargs relaytypes.RelayArgs, pargs relaytypes.PluginArgs) (OCR2VRFProvider, error) {
	configWatcher, err := newOCR2VRFConfigProvider(r.lggr, r.chain, rargs.ContractID)
	if err != nil {
		return nil, err
	}
	contractTransmitter, err := newContractTransmitter(r.lggr, rargs, pargs.TransmitterID, configWatcher)
	if err != nil {
		return nil, err
	}
	return &ocr2vrfProvider{
		configWatcher:       configWatcher,
		contractTransmitter: contractTransmitter,
	}, nil
}

type dkgProvider struct {
	*configWatcher
	contractTransmitter *ContractTransmitter
	pluginConfig        config.PluginConfig
}

func (c *dkgProvider) ContractTransmitter() types.ContractTransmitter {
	return c.contractTransmitter
}

type ocr2vrfProvider struct {
	*configWatcher
	contractTransmitter *ContractTransmitter
}

func (c *ocr2vrfProvider) ContractTransmitter() types.ContractTransmitter {
	return c.contractTransmitter
}

func newOCR2VRFConfigProvider(lggr logger.Logger, chain evm.Chain, contractID string) (*configWatcher, error) {
	if !common.IsHexAddress(contractID) {
		return nil, fmt.Errorf("invalid contract address '%s'", contractID)
	}

	contractAddress := common.HexToAddress(contractID)
	contractABI, err := abi.JSON(strings.NewReader(ocr2aggregator.OCR2AggregatorABI))
	if err != nil {
		return nil, errors.Wrap(err, "could not get OCR2Aggregator ABI JSON")
	}
	configPoller := NewConfigPoller(
		lggr.With("contractID", contractID),
		chain.LogPoller(),
		contractAddress)

	offchainConfigDigester := evmutil.EVMOffchainConfigDigester{
		ChainID:         chain.Config().ChainID().Uint64(),
		ContractAddress: contractAddress,
	}

	return &configWatcher{
		contractAddress:  contractAddress,
		contractABI:      contractABI,
		configPoller:     configPoller,
		offchainDigester: offchainConfigDigester,
		chain:            chain,
	}, nil
}
