package keeper

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/smartcontractkit/sqlx"

	"github.com/smartcontractkit/chainlink/core/chains/evm"
	evmclient "github.com/smartcontractkit/chainlink/core/chains/evm/client"
	"github.com/smartcontractkit/chainlink/core/chains/evm/txmgr"
	registry1_1 "github.com/smartcontractkit/chainlink/core/internal/gethwrappers/generated/keeper_registry_wrapper1_1"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/services/job"
	"github.com/smartcontractkit/chainlink/core/services/keystore/keys/ethkey"
	"github.com/smartcontractkit/chainlink/core/services/pipeline"
)

type RegistryVersion int32

const (
	RegistryVersion_Invalid RegistryVersion = iota
	RegistryVersion_1_0
	RegistryVersion_1_1
	RegistryVersion_1_2
)

// To make sure Delegate struct implements job.Delegate interface
var _ job.Delegate = (*Delegate)(nil)

type Delegate struct {
	logger   logger.Logger
	db       *sqlx.DB
	jrm      job.ORM
	pr       pipeline.Runner
	chainSet evm.ChainSet
}

// NewDelegate is the constructor of Delegate
func NewDelegate(
	db *sqlx.DB,
	jrm job.ORM,
	pr pipeline.Runner,
	logger logger.Logger,
	chainSet evm.ChainSet,
) *Delegate {
	return &Delegate{
		logger:   logger,
		db:       db,
		jrm:      jrm,
		pr:       pr,
		chainSet: chainSet,
	}
}

// JobType returns job type
func (d *Delegate) JobType() job.Type {
	return job.Keeper
}

func (Delegate) AfterJobCreated(spec job.Job) {}

func (Delegate) BeforeJobDeleted(spec job.Job) {}

// ServicesForSpec satisfies the job.Delegate interface.
func (d *Delegate) ServicesForSpec(spec job.Job) (services []job.ServiceCtx, err error) {
	// TODO: we need to fill these out manually, find a better fix
	spec.PipelineSpec.JobName = spec.Name.ValueOrZero()
	spec.PipelineSpec.JobID = spec.ID

	if spec.KeeperSpec == nil {
		return nil, errors.Errorf("Delegate expects a *job.KeeperSpec to be present, got %v", spec)
	}
	chain, err := d.chainSet.Get(spec.KeeperSpec.EVMChainID.ToInt())
	if err != nil {
		return nil, err
	}

	strategy := txmgr.NewQueueingTxStrategy(spec.ExternalJobID, chain.Config().KeeperDefaultTransactionQueueDepth())
	orm := NewORM(d.db, d.logger, chain.Config(), strategy)
	svcLogger := d.logger.With(
		"jobID", spec.ID,
		"registryAddress", spec.KeeperSpec.ContractAddress.Hex(),
	)

	contract, err := registry1_1.NewKeeperRegistry(
		spec.KeeperSpec.ContractAddress.Address(),
		chain.Client(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create keeper registry contract wrapper")
	}

	registryVersion, err := getRegistryVersion(spec.KeeperSpec.ContractAddress, chain.Client())
	fmt.Println("HEEERE", registryVersion, err)
	if err != nil {
		return nil, errors.Wrap(err, "unable to determine version of keeper registry contract")
	}

	minIncomingConfirmations := chain.Config().MinIncomingConfirmations()
	if spec.KeeperSpec.MinIncomingConfirmations != nil {
		minIncomingConfirmations = *spec.KeeperSpec.MinIncomingConfirmations
	}

	registrySynchronizer := NewRegistrySynchronizer(RegistrySynchronizerOptions{
		Job:                      spec,
		ORM:                      orm,
		JRM:                      d.jrm,
		LogBroadcaster:           chain.LogBroadcaster(),
		SyncInterval:             chain.Config().KeeperRegistrySyncInterval(),
		MinIncomingConfirmations: minIncomingConfirmations,
		Logger:                   svcLogger,
		Client:                   chain.Client(),
		Contract:                 contract,
		Version:                  registryVersion,
		SyncUpkeepQueueSize:      chain.Config().KeeperRegistrySyncUpkeepQueueSize(),
	})
	upkeepExecuter := NewUpkeepExecuter(
		spec,
		orm,
		d.pr,
		chain.Client(),
		chain.HeadBroadcaster(),
		chain.TxManager().GetGasEstimator(),
		svcLogger,
		chain.Config(),
	)

	return []job.ServiceCtx{
		registrySynchronizer,
		upkeepExecuter,
	}, nil
}

func getRegistryVersion(registryAddress ethkey.EIP55Address, client evmclient.Client) (RegistryVersion, error) {
	// Use registry 1_1 to get version information
	contract, err := registry1_1.NewKeeperRegistry(
		registryAddress.Address(),
		client,
	)
	if err != nil {
		return RegistryVersion_Invalid, errors.Wrap(err, "unable to create keeper registry contract wrapper")
	}
	typeAndVersion, err := contract.TypeAndVersion(nil)
	if err != nil {
		fmt.Println(err)
		return RegistryVersion_Invalid, errors.Wrap(err, "unable to fetch registry version")
	}
	switch typeAndVersion {
	case "KeeperRegistry 1.1.0":
		return RegistryVersion_1_1, nil
	case "KeeperRegistry 1.2.0":
		return RegistryVersion_1_2, nil
	default:
		return RegistryVersion_Invalid, errors.Errorf("Registry version %s not supported", typeAndVersion)
	}
}
