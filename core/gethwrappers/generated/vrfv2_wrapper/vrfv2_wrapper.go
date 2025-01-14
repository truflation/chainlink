// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package vrfv2_wrapper

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/smartcontractkit/chainlink/core/gethwrappers/generated"
)

var (
	_ = errors.New
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

var VRFV2WrapperMetaData = &bind.MetaData{
	ABI: "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_link\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"_linkEthFeed\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"_coordinator\",\"type\":\"address\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"have\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"want\",\"type\":\"address\"}],\"name\":\"OnlyCoordinatorCanFulfill\",\"type\":\"error\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"from\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"to\",\"type\":\"address\"}],\"name\":\"OwnershipTransferRequested\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"from\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"to\",\"type\":\"address\"}],\"name\":\"OwnershipTransferred\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"uint256\",\"name\":\"requestId\",\"type\":\"uint256\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"consumer\",\"type\":\"address\"}],\"name\":\"WrapperFulfillmentFailed\",\"type\":\"event\"},{\"inputs\":[],\"name\":\"COORDINATOR\",\"outputs\":[{\"internalType\":\"contractExtendedVRFCoordinatorV2Interface\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"LINK\",\"outputs\":[{\"internalType\":\"contractLinkTokenInterface\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"LINK_ETH_FEED\",\"outputs\":[{\"internalType\":\"contractAggregatorV3Interface\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"SUBSCRIPTION_ID\",\"outputs\":[{\"internalType\":\"uint64\",\"name\":\"\",\"type\":\"uint64\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"acceptOwnership\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint32\",\"name\":\"_callbackGasLimit\",\"type\":\"uint32\"}],\"name\":\"calculateRequestPrice\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"disable\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"enable\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint32\",\"name\":\"_callbackGasLimit\",\"type\":\"uint32\"},{\"internalType\":\"uint256\",\"name\":\"_requestGasPriceWei\",\"type\":\"uint256\"}],\"name\":\"estimateRequestPrice\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getConfig\",\"outputs\":[{\"internalType\":\"int256\",\"name\":\"fallbackWeiPerUnitLink\",\"type\":\"int256\"},{\"internalType\":\"uint32\",\"name\":\"stalenessSeconds\",\"type\":\"uint32\"},{\"internalType\":\"uint32\",\"name\":\"fulfillmentFlatFeeLinkPPM\",\"type\":\"uint32\"},{\"internalType\":\"uint32\",\"name\":\"wrapperGasOverhead\",\"type\":\"uint32\"},{\"internalType\":\"uint32\",\"name\":\"coordinatorGasOverhead\",\"type\":\"uint32\"},{\"internalType\":\"uint8\",\"name\":\"wrapperPremiumPercentage\",\"type\":\"uint8\"},{\"internalType\":\"bytes32\",\"name\":\"keyHash\",\"type\":\"bytes32\"},{\"internalType\":\"uint8\",\"name\":\"maxNumWords\",\"type\":\"uint8\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"lastRequestId\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_sender\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"_amount\",\"type\":\"uint256\"},{\"internalType\":\"bytes\",\"name\":\"_data\",\"type\":\"bytes\"}],\"name\":\"onTokenTransfer\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"owner\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"requestId\",\"type\":\"uint256\"},{\"internalType\":\"uint256[]\",\"name\":\"randomWords\",\"type\":\"uint256[]\"}],\"name\":\"rawFulfillRandomWords\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"s_callbacks\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"callbackAddress\",\"type\":\"address\"},{\"internalType\":\"uint32\",\"name\":\"callbackGasLimit\",\"type\":\"uint32\"},{\"internalType\":\"uint256\",\"name\":\"requestGasPrice\",\"type\":\"uint256\"},{\"internalType\":\"int256\",\"name\":\"requestWeiPerUnitLink\",\"type\":\"int256\"},{\"internalType\":\"uint256\",\"name\":\"juelsPaid\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"s_configured\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"s_disabled\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint32\",\"name\":\"_wrapperGasOverhead\",\"type\":\"uint32\"},{\"internalType\":\"uint32\",\"name\":\"_coordinatorGasOverhead\",\"type\":\"uint32\"},{\"internalType\":\"uint8\",\"name\":\"_wrapperPremiumPercentage\",\"type\":\"uint8\"},{\"internalType\":\"bytes32\",\"name\":\"_keyHash\",\"type\":\"bytes32\"},{\"internalType\":\"uint8\",\"name\":\"_maxNumWords\",\"type\":\"uint8\"}],\"name\":\"setConfig\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"to\",\"type\":\"address\"}],\"name\":\"transferOwnership\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"typeAndVersion\",\"outputs\":[{\"internalType\":\"string\",\"name\":\"\",\"type\":\"string\"}],\"stateMutability\":\"pure\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"withdraw\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]",
	Bin: "0x6101206040523480156200001257600080fd5b506040516200228a3803806200228a833981016040819052620000359162000292565b8033806000816200008d5760405162461bcd60e51b815260206004820152601860248201527f43616e6e6f7420736574206f776e657220746f207a65726f000000000000000060448201526064015b60405180910390fd5b600080546001600160a01b0319166001600160a01b0384811691909117909155811615620000c057620000c081620001ca565b5050506001600160a01b0390811660805283811660a05282811660c052811660e08190526040805163288688f960e21b815290516000929163a21a23e4916004808301926020929190829003018187875af115801562000124573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906200014a9190620002dc565b6001600160401b038116610100819052604051631cd0704360e21b815260048101919091523060248201529091506001600160a01b03831690637341c10c90604401600060405180830381600087803b158015620001a757600080fd5b505af1158015620001bc573d6000803e3d6000fd5b50505050505050506200030e565b336001600160a01b03821603620002245760405162461bcd60e51b815260206004820152601760248201527f43616e6e6f74207472616e7366657220746f2073656c66000000000000000000604482015260640162000084565b600180546001600160a01b0319166001600160a01b0383811691821790925560008054604051929316917fed8889f560326eb138920d842192f0eb3dd22b4f139c87a2c57538e05bae12789190a350565b80516001600160a01b03811681146200028d57600080fd5b919050565b600080600060608486031215620002a857600080fd5b620002b38462000275565b9250620002c36020850162000275565b9150620002d36040850162000275565b90509250925092565b600060208284031215620002ef57600080fd5b81516001600160401b03811681146200030757600080fd5b9392505050565b60805160a05160c05160e05161010051611ef862000392600039600081816101810152610be201526000818161026e01528181610ba301528181610edb01528181610fad01526110390152600081816103e701526114cf01526000818161020501528181610a02015261117c0152600081816104f2015261055a0152611ef86000f3fe608060405234801561001057600080fd5b50600436106101775760003560e01c80637fb5d19d116100d8578063ad1783611161008c578063f2fde38b11610066578063f2fde38b146104ab578063f3fef3a3146104be578063fc2a88c3146104d157600080fd5b8063ad178361146103e2578063c15ce4d714610409578063c3f909d41461041c57600080fd5b8063a3907d71116100bd578063a3907d71146103b5578063a4c0ed36146103bd578063a608a1e1146103d057600080fd5b80637fb5d19d146103845780638da5cb5b1461039757600080fd5b80633b2bcbf11161012f57806348baa1c51161011457806348baa1c5146102b157806357a8070a1461035f57806379ba50971461037c57600080fd5b80633b2bcbf1146102695780634306d3541461029057600080fd5b80631b6b6d23116101605780631b6b6d23146102005780631fe543e31461024c5780632f2770db1461026157600080fd5b8063030932bb1461017c578063181f5a77146101c1575b600080fd5b6101a37f000000000000000000000000000000000000000000000000000000000000000081565b60405167ffffffffffffffff90911681526020015b60405180910390f35b604080518082018252601281527f56524656325772617070657220312e302e300000000000000000000000000000602082015290516101b8919061180b565b6102277f000000000000000000000000000000000000000000000000000000000000000081565b60405173ffffffffffffffffffffffffffffffffffffffff90911681526020016101b8565b61025f61025a3660046118ad565b6104da565b005b61025f61059a565b6102277f000000000000000000000000000000000000000000000000000000000000000081565b6102a361029e3660046119a7565b6105d0565b6040519081526020016101b8565b61031b6102bf3660046119c4565b600860205260009081526040902080546001820154600283015460039093015473ffffffffffffffffffffffffffffffffffffffff8316937401000000000000000000000000000000000000000090930463ffffffff16929085565b6040805173ffffffffffffffffffffffffffffffffffffffff909616865263ffffffff9094166020860152928401919091526060830152608082015260a0016101b8565b60035461036c9060ff1681565b60405190151581526020016101b8565b61025f6106d7565b6102a36103923660046119dd565b6107d4565b60005473ffffffffffffffffffffffffffffffffffffffff16610227565b61025f6108da565b61025f6103cb366004611a2d565b61090c565b60035461036c90610100900460ff1681565b6102277f000000000000000000000000000000000000000000000000000000000000000081565b61025f610417366004611ac5565b610d96565b6004546005546006546007546040805194855263ffffffff80851660208701526401000000008504811691860191909152680100000000000000008404811660608601526c01000000000000000000000000840416608085015260ff700100000000000000000000000000000000909304831660a085015260c08401919091521660e0820152610100016101b8565b61025f6104b9366004611b27565b611114565b61025f6104cc366004611b42565b611128565b6102a360025481565b3373ffffffffffffffffffffffffffffffffffffffff7f0000000000000000000000000000000000000000000000000000000000000000161461058c576040517f1cf993f400000000000000000000000000000000000000000000000000000000815233600482015273ffffffffffffffffffffffffffffffffffffffff7f00000000000000000000000000000000000000000000000000000000000000001660248201526044015b60405180910390fd5b61059682826111ee565b5050565b6105a26113f9565b600380547fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00ff16610100179055565b60035460009060ff1661063f576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601960248201527f77726170706572206973206e6f7420636f6e66696775726564000000000000006044820152606401610583565b600354610100900460ff16156106b1576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601360248201527f777261707065722069732064697361626c6564000000000000000000000000006044820152606401610583565b60006106bb61147c565b90506106ce8363ffffffff163a836115e1565b9150505b919050565b60015473ffffffffffffffffffffffffffffffffffffffff163314610758576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601660248201527f4d7573742062652070726f706f736564206f776e6572000000000000000000006044820152606401610583565b60008054337fffffffffffffffffffffffff00000000000000000000000000000000000000008083168217845560018054909116905560405173ffffffffffffffffffffffffffffffffffffffff90921692909183917f8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e091a350565b60035460009060ff16610843576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601960248201527f77726170706572206973206e6f7420636f6e66696775726564000000000000006044820152606401610583565b600354610100900460ff16156108b5576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601360248201527f777261707065722069732064697361626c6564000000000000000000000000006044820152606401610583565b60006108bf61147c565b90506108d28463ffffffff1684836115e1565b949350505050565b6108e26113f9565b600380547fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00ff169055565b60035460ff16610978576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601960248201527f77726170706572206973206e6f7420636f6e66696775726564000000000000006044820152606401610583565b600354610100900460ff16156109ea576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601360248201527f777261707065722069732064697361626c6564000000000000000000000000006044820152606401610583565b3373ffffffffffffffffffffffffffffffffffffffff7f00000000000000000000000000000000000000000000000000000000000000001614610a89576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601760248201527f6f6e6c792063616c6c61626c652066726f6d204c494e4b0000000000000000006044820152606401610583565b60008080610a9984860186611b6e565b9250925092506000610aa961147c565b90506000610abe8563ffffffff163a846115e1565b905080881015610b2a576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152600b60248201527f66656520746f6f206c6f770000000000000000000000000000000000000000006044820152606401610583565b60075460ff1663ffffffff84161115610b9f576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601160248201527f6e756d576f72647320746f6f20686967680000000000000000000000000000006044820152606401610583565b60007f000000000000000000000000000000000000000000000000000000000000000073ffffffffffffffffffffffffffffffffffffffff16635d3b1d306006547f000000000000000000000000000000000000000000000000000000000000000088600560089054906101000a900463ffffffff168b610c209190611be8565b6040517fffffffff0000000000000000000000000000000000000000000000000000000060e087901b168152600481019490945267ffffffffffffffff909216602484015261ffff16604483015263ffffffff90811660648301528716608482015260a4016020604051808303816000875af1158015610ca4573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190610cc89190611c10565b6040805160a08101825273ffffffffffffffffffffffffffffffffffffffff9c8d16815263ffffffff98891660208083019182523a83850190815260608401988952608084019e8f52600086815260089092529390209151825491519e167fffffffffffffffff00000000000000000000000000000000000000000000000090911617740100000000000000000000000000000000000000009d9099169c909c02979097178b55955160018b0155505051600280890191909155955160039097019690965550909255505050565b610d9e6113f9565b6005805460ff808616700100000000000000000000000000000000027fffffffffffffffffffffffffffffff00ffffffffffffffffffffffffffffffff63ffffffff8981166c01000000000000000000000000027fffffffffffffffffffffffffffffffff00000000ffffffffffffffffffffffff918c166801000000000000000002919091167fffffffffffffffffffffffffffffffff0000000000000000ffffffffffffffff909516949094179390931792909216919091179091556006839055600780549183167fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00928316179055600380549091166001179055604080517fc3f909d4000000000000000000000000000000000000000000000000000000008152905173ffffffffffffffffffffffffffffffffffffffff7f0000000000000000000000000000000000000000000000000000000000000000169163c3f909d49160048281019260809291908290030181865afa158015610f26573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190610f4a9190611c29565b50600580547fffffffffffffffffffffffffffffffffffffffffffffffffffffffff000000001663ffffffff929092169190911790555050604080517f356dac7100000000000000000000000000000000000000000000000000000000815290517f000000000000000000000000000000000000000000000000000000000000000073ffffffffffffffffffffffffffffffffffffffff169163356dac719160048083019260209291908290030181865afa15801561100d573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906110319190611c10565b6004819055507f000000000000000000000000000000000000000000000000000000000000000073ffffffffffffffffffffffffffffffffffffffff16635fbbc0d26040518163ffffffff1660e01b815260040161012060405180830381865afa1580156110a3573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906110c79190611c9b565b50506005805463ffffffff909816640100000000027fffffffffffffffffffffffffffffffffffffffffffffffff00000000ffffffff909816979097179096555050505050505050505050565b61111c6113f9565b611125816116ca565b50565b6111306113f9565b6040517fa9059cbb00000000000000000000000000000000000000000000000000000000815273ffffffffffffffffffffffffffffffffffffffff8381166004830152602482018390527f0000000000000000000000000000000000000000000000000000000000000000169063a9059cbb906044016020604051808303816000875af11580156111c5573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906111e99190611d51565b505050565b6000828152600860208181526040808420815160a081018352815473ffffffffffffffffffffffffffffffffffffffff808216835263ffffffff740100000000000000000000000000000000000000008304168387015260018401805495840195909552600284018054606085015260038501805460808601528b8a52979096527fffffffffffffffff00000000000000000000000000000000000000000000000090911690925591859055918490559290915581511661130b576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601160248201527f72657175657374206e6f7420666f756e640000000000000000000000000000006044820152606401610583565b600080631fe543e360e01b8585604051602401611329929190611d73565b604051602081830303815290604052907bffffffffffffffffffffffffffffffffffffffffffffffffffffffff19166020820180517bffffffffffffffffffffffffffffffffffffffffffffffffffffffff8381831617835250505050905060006113a3846020015163ffffffff168560000151846117bf565b9050806113f157835160405173ffffffffffffffffffffffffffffffffffffffff9091169087907fc551b83c151f2d1c7eeb938ac59008e0409f1c1dc1e2f112449d4d79b458902290600090a35b505050505050565b60005473ffffffffffffffffffffffffffffffffffffffff16331461147a576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601660248201527f4f6e6c792063616c6c61626c65206279206f776e6572000000000000000000006044820152606401610583565b565b600554604080517ffeaf968c000000000000000000000000000000000000000000000000000000008152905160009263ffffffff161515918391829173ffffffffffffffffffffffffffffffffffffffff7f0000000000000000000000000000000000000000000000000000000000000000169163feaf968c9160048082019260a0929091908290030181865afa15801561151b573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061153f9190611ddb565b509450909250849150508015611565575061155a8242611e1f565b60055463ffffffff16105b1561156f57506004545b60008112156115da576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601660248201527f496e76616c6964204c494e4b20776569207072696365000000000000000000006044820152606401610583565b9392505050565b6005546000908190839063ffffffff6c01000000000000000000000000820481169161161b91680100000000000000009091041688611e36565b6116259190611e36565b61163786670de0b6b3a7640000611e4e565b6116419190611e4e565b61164b9190611e8b565b60055490915060009060649061167890700100000000000000000000000000000000900460ff1682611ec6565b6116859060ff1684611e4e565b61168f9190611e8b565b6005549091506000906116b590640100000000900463ffffffff1664e8d4a51000611e4e565b6116bf9083611e36565b979650505050505050565b3373ffffffffffffffffffffffffffffffffffffffff821603611749576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601760248201527f43616e6e6f74207472616e7366657220746f2073656c660000000000000000006044820152606401610583565b600180547fffffffffffffffffffffffff00000000000000000000000000000000000000001673ffffffffffffffffffffffffffffffffffffffff83811691821790925560008054604051929316917fed8889f560326eb138920d842192f0eb3dd22b4f139c87a2c57538e05bae12789190a350565b60005a6113888110156117d157600080fd5b6113888103905084604082048203116117e957600080fd5b50823b6117f557600080fd5b60008083516020850160008789f1949350505050565b600060208083528351808285015260005b818110156118385785810183015185820160400152820161181c565b8181111561184a576000604083870101525b50601f017fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe016929092016040019392505050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b600080604083850312156118c057600080fd5b8235915060208084013567ffffffffffffffff808211156118e057600080fd5b818601915086601f8301126118f457600080fd5b8135818111156119065761190661187e565b8060051b6040517fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0603f830116810181811085821117156119495761194961187e565b60405291825284820192508381018501918983111561196757600080fd5b938501935b828510156119855784358452938501939285019261196c565b8096505050505050509250929050565b63ffffffff8116811461112557600080fd5b6000602082840312156119b957600080fd5b81356115da81611995565b6000602082840312156119d657600080fd5b5035919050565b600080604083850312156119f057600080fd5b82356119fb81611995565b946020939093013593505050565b803573ffffffffffffffffffffffffffffffffffffffff811681146106d257600080fd5b60008060008060608587031215611a4357600080fd5b611a4c85611a09565b935060208501359250604085013567ffffffffffffffff80821115611a7057600080fd5b818701915087601f830112611a8457600080fd5b813581811115611a9357600080fd5b886020828501011115611aa557600080fd5b95989497505060200194505050565b803560ff811681146106d257600080fd5b600080600080600060a08688031215611add57600080fd5b8535611ae881611995565b94506020860135611af881611995565b9350611b0660408701611ab4565b925060608601359150611b1b60808701611ab4565b90509295509295909350565b600060208284031215611b3957600080fd5b6115da82611a09565b60008060408385031215611b5557600080fd5b6119fb83611a09565b61ffff8116811461112557600080fd5b600080600060608486031215611b8357600080fd5b8335611b8e81611995565b92506020840135611b9e81611b5e565b91506040840135611bae81611995565b809150509250925092565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b600063ffffffff808316818516808303821115611c0757611c07611bb9565b01949350505050565b600060208284031215611c2257600080fd5b5051919050565b60008060008060808587031215611c3f57600080fd5b8451611c4a81611b5e565b6020860151909450611c5b81611995565b6040860151909350611c6c81611995565b6060860151909250611c7d81611995565b939692955090935050565b805162ffffff811681146106d257600080fd5b60008060008060008060008060006101208a8c031215611cba57600080fd5b8951611cc581611995565b60208b0151909950611cd681611995565b60408b0151909850611ce781611995565b60608b0151909750611cf881611995565b60808b0151909650611d0981611995565b9450611d1760a08b01611c88565b9350611d2560c08b01611c88565b9250611d3360e08b01611c88565b9150611d426101008b01611c88565b90509295985092959850929598565b600060208284031215611d6357600080fd5b815180151581146115da57600080fd5b6000604082018483526020604081850152818551808452606086019150828701935060005b81811015611db457845183529383019391830191600101611d98565b5090979650505050505050565b805169ffffffffffffffffffff811681146106d257600080fd5b600080600080600060a08688031215611df357600080fd5b611dfc86611dc1565b9450602086015193506040860151925060608601519150611b1b60808701611dc1565b600082821015611e3157611e31611bb9565b500390565b60008219821115611e4957611e49611bb9565b500190565b6000817fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0483118215151615611e8657611e86611bb9565b500290565b600082611ec1577f4e487b7100000000000000000000000000000000000000000000000000000000600052601260045260246000fd5b500490565b600060ff821660ff84168060ff03821115611ee357611ee3611bb9565b01939250505056fea164736f6c634300080f000a",
}

var VRFV2WrapperABI = VRFV2WrapperMetaData.ABI

var VRFV2WrapperBin = VRFV2WrapperMetaData.Bin

func DeployVRFV2Wrapper(auth *bind.TransactOpts, backend bind.ContractBackend, _link common.Address, _linkEthFeed common.Address, _coordinator common.Address) (common.Address, *types.Transaction, *VRFV2Wrapper, error) {
	parsed, err := VRFV2WrapperMetaData.GetAbi()
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	if parsed == nil {
		return common.Address{}, nil, nil, errors.New("GetABI returned nil")
	}

	address, tx, contract, err := bind.DeployContract(auth, *parsed, common.FromHex(VRFV2WrapperBin), backend, _link, _linkEthFeed, _coordinator)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &VRFV2Wrapper{VRFV2WrapperCaller: VRFV2WrapperCaller{contract: contract}, VRFV2WrapperTransactor: VRFV2WrapperTransactor{contract: contract}, VRFV2WrapperFilterer: VRFV2WrapperFilterer{contract: contract}}, nil
}

type VRFV2Wrapper struct {
	address common.Address
	abi     abi.ABI
	VRFV2WrapperCaller
	VRFV2WrapperTransactor
	VRFV2WrapperFilterer
}

type VRFV2WrapperCaller struct {
	contract *bind.BoundContract
}

type VRFV2WrapperTransactor struct {
	contract *bind.BoundContract
}

type VRFV2WrapperFilterer struct {
	contract *bind.BoundContract
}

type VRFV2WrapperSession struct {
	Contract     *VRFV2Wrapper
	CallOpts     bind.CallOpts
	TransactOpts bind.TransactOpts
}

type VRFV2WrapperCallerSession struct {
	Contract *VRFV2WrapperCaller
	CallOpts bind.CallOpts
}

type VRFV2WrapperTransactorSession struct {
	Contract     *VRFV2WrapperTransactor
	TransactOpts bind.TransactOpts
}

type VRFV2WrapperRaw struct {
	Contract *VRFV2Wrapper
}

type VRFV2WrapperCallerRaw struct {
	Contract *VRFV2WrapperCaller
}

type VRFV2WrapperTransactorRaw struct {
	Contract *VRFV2WrapperTransactor
}

func NewVRFV2Wrapper(address common.Address, backend bind.ContractBackend) (*VRFV2Wrapper, error) {
	abi, err := abi.JSON(strings.NewReader(VRFV2WrapperABI))
	if err != nil {
		return nil, err
	}
	contract, err := bindVRFV2Wrapper(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &VRFV2Wrapper{address: address, abi: abi, VRFV2WrapperCaller: VRFV2WrapperCaller{contract: contract}, VRFV2WrapperTransactor: VRFV2WrapperTransactor{contract: contract}, VRFV2WrapperFilterer: VRFV2WrapperFilterer{contract: contract}}, nil
}

func NewVRFV2WrapperCaller(address common.Address, caller bind.ContractCaller) (*VRFV2WrapperCaller, error) {
	contract, err := bindVRFV2Wrapper(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &VRFV2WrapperCaller{contract: contract}, nil
}

func NewVRFV2WrapperTransactor(address common.Address, transactor bind.ContractTransactor) (*VRFV2WrapperTransactor, error) {
	contract, err := bindVRFV2Wrapper(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &VRFV2WrapperTransactor{contract: contract}, nil
}

func NewVRFV2WrapperFilterer(address common.Address, filterer bind.ContractFilterer) (*VRFV2WrapperFilterer, error) {
	contract, err := bindVRFV2Wrapper(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &VRFV2WrapperFilterer{contract: contract}, nil
}

func bindVRFV2Wrapper(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(VRFV2WrapperABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

func (_VRFV2Wrapper *VRFV2WrapperRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _VRFV2Wrapper.Contract.VRFV2WrapperCaller.contract.Call(opts, result, method, params...)
}

func (_VRFV2Wrapper *VRFV2WrapperRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.VRFV2WrapperTransactor.contract.Transfer(opts)
}

func (_VRFV2Wrapper *VRFV2WrapperRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.VRFV2WrapperTransactor.contract.Transact(opts, method, params...)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _VRFV2Wrapper.Contract.contract.Call(opts, result, method, params...)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.contract.Transfer(opts)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.contract.Transact(opts, method, params...)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) COORDINATOR(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "COORDINATOR")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) COORDINATOR() (common.Address, error) {
	return _VRFV2Wrapper.Contract.COORDINATOR(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) COORDINATOR() (common.Address, error) {
	return _VRFV2Wrapper.Contract.COORDINATOR(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) LINK(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "LINK")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) LINK() (common.Address, error) {
	return _VRFV2Wrapper.Contract.LINK(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) LINK() (common.Address, error) {
	return _VRFV2Wrapper.Contract.LINK(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) LINKETHFEED(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "LINK_ETH_FEED")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) LINKETHFEED() (common.Address, error) {
	return _VRFV2Wrapper.Contract.LINKETHFEED(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) LINKETHFEED() (common.Address, error) {
	return _VRFV2Wrapper.Contract.LINKETHFEED(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) SUBSCRIPTIONID(opts *bind.CallOpts) (uint64, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "SUBSCRIPTION_ID")

	if err != nil {
		return *new(uint64), err
	}

	out0 := *abi.ConvertType(out[0], new(uint64)).(*uint64)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) SUBSCRIPTIONID() (uint64, error) {
	return _VRFV2Wrapper.Contract.SUBSCRIPTIONID(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) SUBSCRIPTIONID() (uint64, error) {
	return _VRFV2Wrapper.Contract.SUBSCRIPTIONID(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) CalculateRequestPrice(opts *bind.CallOpts, _callbackGasLimit uint32) (*big.Int, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "calculateRequestPrice", _callbackGasLimit)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) CalculateRequestPrice(_callbackGasLimit uint32) (*big.Int, error) {
	return _VRFV2Wrapper.Contract.CalculateRequestPrice(&_VRFV2Wrapper.CallOpts, _callbackGasLimit)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) CalculateRequestPrice(_callbackGasLimit uint32) (*big.Int, error) {
	return _VRFV2Wrapper.Contract.CalculateRequestPrice(&_VRFV2Wrapper.CallOpts, _callbackGasLimit)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) EstimateRequestPrice(opts *bind.CallOpts, _callbackGasLimit uint32, _requestGasPriceWei *big.Int) (*big.Int, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "estimateRequestPrice", _callbackGasLimit, _requestGasPriceWei)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) EstimateRequestPrice(_callbackGasLimit uint32, _requestGasPriceWei *big.Int) (*big.Int, error) {
	return _VRFV2Wrapper.Contract.EstimateRequestPrice(&_VRFV2Wrapper.CallOpts, _callbackGasLimit, _requestGasPriceWei)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) EstimateRequestPrice(_callbackGasLimit uint32, _requestGasPriceWei *big.Int) (*big.Int, error) {
	return _VRFV2Wrapper.Contract.EstimateRequestPrice(&_VRFV2Wrapper.CallOpts, _callbackGasLimit, _requestGasPriceWei)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) GetConfig(opts *bind.CallOpts) (GetConfig,

	error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "getConfig")

	outstruct := new(GetConfig)
	if err != nil {
		return *outstruct, err
	}

	outstruct.FallbackWeiPerUnitLink = *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	outstruct.StalenessSeconds = *abi.ConvertType(out[1], new(uint32)).(*uint32)
	outstruct.FulfillmentFlatFeeLinkPPM = *abi.ConvertType(out[2], new(uint32)).(*uint32)
	outstruct.WrapperGasOverhead = *abi.ConvertType(out[3], new(uint32)).(*uint32)
	outstruct.CoordinatorGasOverhead = *abi.ConvertType(out[4], new(uint32)).(*uint32)
	outstruct.WrapperPremiumPercentage = *abi.ConvertType(out[5], new(uint8)).(*uint8)
	outstruct.KeyHash = *abi.ConvertType(out[6], new([32]byte)).(*[32]byte)
	outstruct.MaxNumWords = *abi.ConvertType(out[7], new(uint8)).(*uint8)

	return *outstruct, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) GetConfig() (GetConfig,

	error) {
	return _VRFV2Wrapper.Contract.GetConfig(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) GetConfig() (GetConfig,

	error) {
	return _VRFV2Wrapper.Contract.GetConfig(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) LastRequestId(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "lastRequestId")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) LastRequestId() (*big.Int, error) {
	return _VRFV2Wrapper.Contract.LastRequestId(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) LastRequestId() (*big.Int, error) {
	return _VRFV2Wrapper.Contract.LastRequestId(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) Owner(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "owner")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) Owner() (common.Address, error) {
	return _VRFV2Wrapper.Contract.Owner(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) Owner() (common.Address, error) {
	return _VRFV2Wrapper.Contract.Owner(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) SCallbacks(opts *bind.CallOpts, arg0 *big.Int) (SCallbacks,

	error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "s_callbacks", arg0)

	outstruct := new(SCallbacks)
	if err != nil {
		return *outstruct, err
	}

	outstruct.CallbackAddress = *abi.ConvertType(out[0], new(common.Address)).(*common.Address)
	outstruct.CallbackGasLimit = *abi.ConvertType(out[1], new(uint32)).(*uint32)
	outstruct.RequestGasPrice = *abi.ConvertType(out[2], new(*big.Int)).(**big.Int)
	outstruct.RequestWeiPerUnitLink = *abi.ConvertType(out[3], new(*big.Int)).(**big.Int)
	outstruct.JuelsPaid = *abi.ConvertType(out[4], new(*big.Int)).(**big.Int)

	return *outstruct, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) SCallbacks(arg0 *big.Int) (SCallbacks,

	error) {
	return _VRFV2Wrapper.Contract.SCallbacks(&_VRFV2Wrapper.CallOpts, arg0)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) SCallbacks(arg0 *big.Int) (SCallbacks,

	error) {
	return _VRFV2Wrapper.Contract.SCallbacks(&_VRFV2Wrapper.CallOpts, arg0)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) SConfigured(opts *bind.CallOpts) (bool, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "s_configured")

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) SConfigured() (bool, error) {
	return _VRFV2Wrapper.Contract.SConfigured(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) SConfigured() (bool, error) {
	return _VRFV2Wrapper.Contract.SConfigured(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) SDisabled(opts *bind.CallOpts) (bool, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "s_disabled")

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) SDisabled() (bool, error) {
	return _VRFV2Wrapper.Contract.SDisabled(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) SDisabled() (bool, error) {
	return _VRFV2Wrapper.Contract.SDisabled(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCaller) TypeAndVersion(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _VRFV2Wrapper.contract.Call(opts, &out, "typeAndVersion")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

func (_VRFV2Wrapper *VRFV2WrapperSession) TypeAndVersion() (string, error) {
	return _VRFV2Wrapper.Contract.TypeAndVersion(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperCallerSession) TypeAndVersion() (string, error) {
	return _VRFV2Wrapper.Contract.TypeAndVersion(&_VRFV2Wrapper.CallOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactor) AcceptOwnership(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _VRFV2Wrapper.contract.Transact(opts, "acceptOwnership")
}

func (_VRFV2Wrapper *VRFV2WrapperSession) AcceptOwnership() (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.AcceptOwnership(&_VRFV2Wrapper.TransactOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorSession) AcceptOwnership() (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.AcceptOwnership(&_VRFV2Wrapper.TransactOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactor) Disable(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _VRFV2Wrapper.contract.Transact(opts, "disable")
}

func (_VRFV2Wrapper *VRFV2WrapperSession) Disable() (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.Disable(&_VRFV2Wrapper.TransactOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorSession) Disable() (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.Disable(&_VRFV2Wrapper.TransactOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactor) Enable(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _VRFV2Wrapper.contract.Transact(opts, "enable")
}

func (_VRFV2Wrapper *VRFV2WrapperSession) Enable() (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.Enable(&_VRFV2Wrapper.TransactOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorSession) Enable() (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.Enable(&_VRFV2Wrapper.TransactOpts)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactor) OnTokenTransfer(opts *bind.TransactOpts, _sender common.Address, _amount *big.Int, _data []byte) (*types.Transaction, error) {
	return _VRFV2Wrapper.contract.Transact(opts, "onTokenTransfer", _sender, _amount, _data)
}

func (_VRFV2Wrapper *VRFV2WrapperSession) OnTokenTransfer(_sender common.Address, _amount *big.Int, _data []byte) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.OnTokenTransfer(&_VRFV2Wrapper.TransactOpts, _sender, _amount, _data)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorSession) OnTokenTransfer(_sender common.Address, _amount *big.Int, _data []byte) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.OnTokenTransfer(&_VRFV2Wrapper.TransactOpts, _sender, _amount, _data)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactor) RawFulfillRandomWords(opts *bind.TransactOpts, requestId *big.Int, randomWords []*big.Int) (*types.Transaction, error) {
	return _VRFV2Wrapper.contract.Transact(opts, "rawFulfillRandomWords", requestId, randomWords)
}

func (_VRFV2Wrapper *VRFV2WrapperSession) RawFulfillRandomWords(requestId *big.Int, randomWords []*big.Int) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.RawFulfillRandomWords(&_VRFV2Wrapper.TransactOpts, requestId, randomWords)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorSession) RawFulfillRandomWords(requestId *big.Int, randomWords []*big.Int) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.RawFulfillRandomWords(&_VRFV2Wrapper.TransactOpts, requestId, randomWords)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactor) SetConfig(opts *bind.TransactOpts, _wrapperGasOverhead uint32, _coordinatorGasOverhead uint32, _wrapperPremiumPercentage uint8, _keyHash [32]byte, _maxNumWords uint8) (*types.Transaction, error) {
	return _VRFV2Wrapper.contract.Transact(opts, "setConfig", _wrapperGasOverhead, _coordinatorGasOverhead, _wrapperPremiumPercentage, _keyHash, _maxNumWords)
}

func (_VRFV2Wrapper *VRFV2WrapperSession) SetConfig(_wrapperGasOverhead uint32, _coordinatorGasOverhead uint32, _wrapperPremiumPercentage uint8, _keyHash [32]byte, _maxNumWords uint8) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.SetConfig(&_VRFV2Wrapper.TransactOpts, _wrapperGasOverhead, _coordinatorGasOverhead, _wrapperPremiumPercentage, _keyHash, _maxNumWords)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorSession) SetConfig(_wrapperGasOverhead uint32, _coordinatorGasOverhead uint32, _wrapperPremiumPercentage uint8, _keyHash [32]byte, _maxNumWords uint8) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.SetConfig(&_VRFV2Wrapper.TransactOpts, _wrapperGasOverhead, _coordinatorGasOverhead, _wrapperPremiumPercentage, _keyHash, _maxNumWords)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactor) TransferOwnership(opts *bind.TransactOpts, to common.Address) (*types.Transaction, error) {
	return _VRFV2Wrapper.contract.Transact(opts, "transferOwnership", to)
}

func (_VRFV2Wrapper *VRFV2WrapperSession) TransferOwnership(to common.Address) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.TransferOwnership(&_VRFV2Wrapper.TransactOpts, to)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorSession) TransferOwnership(to common.Address) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.TransferOwnership(&_VRFV2Wrapper.TransactOpts, to)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactor) Withdraw(opts *bind.TransactOpts, _recipient common.Address, _amount *big.Int) (*types.Transaction, error) {
	return _VRFV2Wrapper.contract.Transact(opts, "withdraw", _recipient, _amount)
}

func (_VRFV2Wrapper *VRFV2WrapperSession) Withdraw(_recipient common.Address, _amount *big.Int) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.Withdraw(&_VRFV2Wrapper.TransactOpts, _recipient, _amount)
}

func (_VRFV2Wrapper *VRFV2WrapperTransactorSession) Withdraw(_recipient common.Address, _amount *big.Int) (*types.Transaction, error) {
	return _VRFV2Wrapper.Contract.Withdraw(&_VRFV2Wrapper.TransactOpts, _recipient, _amount)
}

type VRFV2WrapperOwnershipTransferRequestedIterator struct {
	Event *VRFV2WrapperOwnershipTransferRequested

	contract *bind.BoundContract
	event    string

	logs chan types.Log
	sub  ethereum.Subscription
	done bool
	fail error
}

func (it *VRFV2WrapperOwnershipTransferRequestedIterator) Next() bool {

	if it.fail != nil {
		return false
	}

	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(VRFV2WrapperOwnershipTransferRequested)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}

	select {
	case log := <-it.logs:
		it.Event = new(VRFV2WrapperOwnershipTransferRequested)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

func (it *VRFV2WrapperOwnershipTransferRequestedIterator) Error() error {
	return it.fail
}

func (it *VRFV2WrapperOwnershipTransferRequestedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

type VRFV2WrapperOwnershipTransferRequested struct {
	From common.Address
	To   common.Address
	Raw  types.Log
}

func (_VRFV2Wrapper *VRFV2WrapperFilterer) FilterOwnershipTransferRequested(opts *bind.FilterOpts, from []common.Address, to []common.Address) (*VRFV2WrapperOwnershipTransferRequestedIterator, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _VRFV2Wrapper.contract.FilterLogs(opts, "OwnershipTransferRequested", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return &VRFV2WrapperOwnershipTransferRequestedIterator{contract: _VRFV2Wrapper.contract, event: "OwnershipTransferRequested", logs: logs, sub: sub}, nil
}

func (_VRFV2Wrapper *VRFV2WrapperFilterer) WatchOwnershipTransferRequested(opts *bind.WatchOpts, sink chan<- *VRFV2WrapperOwnershipTransferRequested, from []common.Address, to []common.Address) (event.Subscription, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _VRFV2Wrapper.contract.WatchLogs(opts, "OwnershipTransferRequested", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:

				event := new(VRFV2WrapperOwnershipTransferRequested)
				if err := _VRFV2Wrapper.contract.UnpackLog(event, "OwnershipTransferRequested", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

func (_VRFV2Wrapper *VRFV2WrapperFilterer) ParseOwnershipTransferRequested(log types.Log) (*VRFV2WrapperOwnershipTransferRequested, error) {
	event := new(VRFV2WrapperOwnershipTransferRequested)
	if err := _VRFV2Wrapper.contract.UnpackLog(event, "OwnershipTransferRequested", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

type VRFV2WrapperOwnershipTransferredIterator struct {
	Event *VRFV2WrapperOwnershipTransferred

	contract *bind.BoundContract
	event    string

	logs chan types.Log
	sub  ethereum.Subscription
	done bool
	fail error
}

func (it *VRFV2WrapperOwnershipTransferredIterator) Next() bool {

	if it.fail != nil {
		return false
	}

	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(VRFV2WrapperOwnershipTransferred)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}

	select {
	case log := <-it.logs:
		it.Event = new(VRFV2WrapperOwnershipTransferred)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

func (it *VRFV2WrapperOwnershipTransferredIterator) Error() error {
	return it.fail
}

func (it *VRFV2WrapperOwnershipTransferredIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

type VRFV2WrapperOwnershipTransferred struct {
	From common.Address
	To   common.Address
	Raw  types.Log
}

func (_VRFV2Wrapper *VRFV2WrapperFilterer) FilterOwnershipTransferred(opts *bind.FilterOpts, from []common.Address, to []common.Address) (*VRFV2WrapperOwnershipTransferredIterator, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _VRFV2Wrapper.contract.FilterLogs(opts, "OwnershipTransferred", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return &VRFV2WrapperOwnershipTransferredIterator{contract: _VRFV2Wrapper.contract, event: "OwnershipTransferred", logs: logs, sub: sub}, nil
}

func (_VRFV2Wrapper *VRFV2WrapperFilterer) WatchOwnershipTransferred(opts *bind.WatchOpts, sink chan<- *VRFV2WrapperOwnershipTransferred, from []common.Address, to []common.Address) (event.Subscription, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _VRFV2Wrapper.contract.WatchLogs(opts, "OwnershipTransferred", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:

				event := new(VRFV2WrapperOwnershipTransferred)
				if err := _VRFV2Wrapper.contract.UnpackLog(event, "OwnershipTransferred", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

func (_VRFV2Wrapper *VRFV2WrapperFilterer) ParseOwnershipTransferred(log types.Log) (*VRFV2WrapperOwnershipTransferred, error) {
	event := new(VRFV2WrapperOwnershipTransferred)
	if err := _VRFV2Wrapper.contract.UnpackLog(event, "OwnershipTransferred", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

type VRFV2WrapperWrapperFulfillmentFailedIterator struct {
	Event *VRFV2WrapperWrapperFulfillmentFailed

	contract *bind.BoundContract
	event    string

	logs chan types.Log
	sub  ethereum.Subscription
	done bool
	fail error
}

func (it *VRFV2WrapperWrapperFulfillmentFailedIterator) Next() bool {

	if it.fail != nil {
		return false
	}

	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(VRFV2WrapperWrapperFulfillmentFailed)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}

	select {
	case log := <-it.logs:
		it.Event = new(VRFV2WrapperWrapperFulfillmentFailed)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

func (it *VRFV2WrapperWrapperFulfillmentFailedIterator) Error() error {
	return it.fail
}

func (it *VRFV2WrapperWrapperFulfillmentFailedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

type VRFV2WrapperWrapperFulfillmentFailed struct {
	RequestId *big.Int
	Consumer  common.Address
	Raw       types.Log
}

func (_VRFV2Wrapper *VRFV2WrapperFilterer) FilterWrapperFulfillmentFailed(opts *bind.FilterOpts, requestId []*big.Int, consumer []common.Address) (*VRFV2WrapperWrapperFulfillmentFailedIterator, error) {

	var requestIdRule []interface{}
	for _, requestIdItem := range requestId {
		requestIdRule = append(requestIdRule, requestIdItem)
	}
	var consumerRule []interface{}
	for _, consumerItem := range consumer {
		consumerRule = append(consumerRule, consumerItem)
	}

	logs, sub, err := _VRFV2Wrapper.contract.FilterLogs(opts, "WrapperFulfillmentFailed", requestIdRule, consumerRule)
	if err != nil {
		return nil, err
	}
	return &VRFV2WrapperWrapperFulfillmentFailedIterator{contract: _VRFV2Wrapper.contract, event: "WrapperFulfillmentFailed", logs: logs, sub: sub}, nil
}

func (_VRFV2Wrapper *VRFV2WrapperFilterer) WatchWrapperFulfillmentFailed(opts *bind.WatchOpts, sink chan<- *VRFV2WrapperWrapperFulfillmentFailed, requestId []*big.Int, consumer []common.Address) (event.Subscription, error) {

	var requestIdRule []interface{}
	for _, requestIdItem := range requestId {
		requestIdRule = append(requestIdRule, requestIdItem)
	}
	var consumerRule []interface{}
	for _, consumerItem := range consumer {
		consumerRule = append(consumerRule, consumerItem)
	}

	logs, sub, err := _VRFV2Wrapper.contract.WatchLogs(opts, "WrapperFulfillmentFailed", requestIdRule, consumerRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:

				event := new(VRFV2WrapperWrapperFulfillmentFailed)
				if err := _VRFV2Wrapper.contract.UnpackLog(event, "WrapperFulfillmentFailed", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

func (_VRFV2Wrapper *VRFV2WrapperFilterer) ParseWrapperFulfillmentFailed(log types.Log) (*VRFV2WrapperWrapperFulfillmentFailed, error) {
	event := new(VRFV2WrapperWrapperFulfillmentFailed)
	if err := _VRFV2Wrapper.contract.UnpackLog(event, "WrapperFulfillmentFailed", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

type GetConfig struct {
	FallbackWeiPerUnitLink    *big.Int
	StalenessSeconds          uint32
	FulfillmentFlatFeeLinkPPM uint32
	WrapperGasOverhead        uint32
	CoordinatorGasOverhead    uint32
	WrapperPremiumPercentage  uint8
	KeyHash                   [32]byte
	MaxNumWords               uint8
}
type SCallbacks struct {
	CallbackAddress       common.Address
	CallbackGasLimit      uint32
	RequestGasPrice       *big.Int
	RequestWeiPerUnitLink *big.Int
	JuelsPaid             *big.Int
}

func (_VRFV2Wrapper *VRFV2Wrapper) ParseLog(log types.Log) (generated.AbigenLog, error) {
	switch log.Topics[0] {
	case _VRFV2Wrapper.abi.Events["OwnershipTransferRequested"].ID:
		return _VRFV2Wrapper.ParseOwnershipTransferRequested(log)
	case _VRFV2Wrapper.abi.Events["OwnershipTransferred"].ID:
		return _VRFV2Wrapper.ParseOwnershipTransferred(log)
	case _VRFV2Wrapper.abi.Events["WrapperFulfillmentFailed"].ID:
		return _VRFV2Wrapper.ParseWrapperFulfillmentFailed(log)

	default:
		return nil, fmt.Errorf("abigen wrapper received unknown log topic: %v", log.Topics[0])
	}
}

func (VRFV2WrapperOwnershipTransferRequested) Topic() common.Hash {
	return common.HexToHash("0xed8889f560326eb138920d842192f0eb3dd22b4f139c87a2c57538e05bae1278")
}

func (VRFV2WrapperOwnershipTransferred) Topic() common.Hash {
	return common.HexToHash("0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0")
}

func (VRFV2WrapperWrapperFulfillmentFailed) Topic() common.Hash {
	return common.HexToHash("0xc551b83c151f2d1c7eeb938ac59008e0409f1c1dc1e2f112449d4d79b4589022")
}

func (_VRFV2Wrapper *VRFV2Wrapper) Address() common.Address {
	return _VRFV2Wrapper.address
}

type VRFV2WrapperInterface interface {
	COORDINATOR(opts *bind.CallOpts) (common.Address, error)

	LINK(opts *bind.CallOpts) (common.Address, error)

	LINKETHFEED(opts *bind.CallOpts) (common.Address, error)

	SUBSCRIPTIONID(opts *bind.CallOpts) (uint64, error)

	CalculateRequestPrice(opts *bind.CallOpts, _callbackGasLimit uint32) (*big.Int, error)

	EstimateRequestPrice(opts *bind.CallOpts, _callbackGasLimit uint32, _requestGasPriceWei *big.Int) (*big.Int, error)

	GetConfig(opts *bind.CallOpts) (GetConfig,

		error)

	LastRequestId(opts *bind.CallOpts) (*big.Int, error)

	Owner(opts *bind.CallOpts) (common.Address, error)

	SCallbacks(opts *bind.CallOpts, arg0 *big.Int) (SCallbacks,

		error)

	SConfigured(opts *bind.CallOpts) (bool, error)

	SDisabled(opts *bind.CallOpts) (bool, error)

	TypeAndVersion(opts *bind.CallOpts) (string, error)

	AcceptOwnership(opts *bind.TransactOpts) (*types.Transaction, error)

	Disable(opts *bind.TransactOpts) (*types.Transaction, error)

	Enable(opts *bind.TransactOpts) (*types.Transaction, error)

	OnTokenTransfer(opts *bind.TransactOpts, _sender common.Address, _amount *big.Int, _data []byte) (*types.Transaction, error)

	RawFulfillRandomWords(opts *bind.TransactOpts, requestId *big.Int, randomWords []*big.Int) (*types.Transaction, error)

	SetConfig(opts *bind.TransactOpts, _wrapperGasOverhead uint32, _coordinatorGasOverhead uint32, _wrapperPremiumPercentage uint8, _keyHash [32]byte, _maxNumWords uint8) (*types.Transaction, error)

	TransferOwnership(opts *bind.TransactOpts, to common.Address) (*types.Transaction, error)

	Withdraw(opts *bind.TransactOpts, _recipient common.Address, _amount *big.Int) (*types.Transaction, error)

	FilterOwnershipTransferRequested(opts *bind.FilterOpts, from []common.Address, to []common.Address) (*VRFV2WrapperOwnershipTransferRequestedIterator, error)

	WatchOwnershipTransferRequested(opts *bind.WatchOpts, sink chan<- *VRFV2WrapperOwnershipTransferRequested, from []common.Address, to []common.Address) (event.Subscription, error)

	ParseOwnershipTransferRequested(log types.Log) (*VRFV2WrapperOwnershipTransferRequested, error)

	FilterOwnershipTransferred(opts *bind.FilterOpts, from []common.Address, to []common.Address) (*VRFV2WrapperOwnershipTransferredIterator, error)

	WatchOwnershipTransferred(opts *bind.WatchOpts, sink chan<- *VRFV2WrapperOwnershipTransferred, from []common.Address, to []common.Address) (event.Subscription, error)

	ParseOwnershipTransferred(log types.Log) (*VRFV2WrapperOwnershipTransferred, error)

	FilterWrapperFulfillmentFailed(opts *bind.FilterOpts, requestId []*big.Int, consumer []common.Address) (*VRFV2WrapperWrapperFulfillmentFailedIterator, error)

	WatchWrapperFulfillmentFailed(opts *bind.WatchOpts, sink chan<- *VRFV2WrapperWrapperFulfillmentFailed, requestId []*big.Int, consumer []common.Address) (event.Subscription, error)

	ParseWrapperFulfillmentFailed(log types.Log) (*VRFV2WrapperWrapperFulfillmentFailed, error)

	ParseLog(log types.Log) (generated.AbigenLog, error)

	Address() common.Address
}
