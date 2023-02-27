package main

import (
	"context"
	"flag"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"

	"github.com/alexeykiselev/waves-tx-graph/internal"
	"github.com/pkg/errors"
	"github.com/wavesplatform/gowaves/pkg/crypto"
	"github.com/wavesplatform/gowaves/pkg/grpc/generated/waves"
	"github.com/wavesplatform/gowaves/pkg/grpc/generated/waves/events"
	bu "github.com/wavesplatform/gowaves/pkg/grpc/generated/waves/events/grpc"
	"github.com/wavesplatform/gowaves/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const tenMB = 10 * 1024 * 1024

func main() {
	if err := run(); err != nil {
		switch err {
		case context.Canceled:
			log.Println("User termination")
			os.Exit(130)
		default:
			log.Printf("Error: %s", err.Error())
			os.Exit(1)
		}
	}
}

func run() error {
	var (
		nodeURL     string
		storagePath string
	)
	flag.StringVar(&nodeURL, "node", "", "Waves Blockchain Updates Node's gRPC API URL")
	flag.StringVar(&storagePath, "storage", "", "Path to application's storage")
	flag.Parse()

	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()

	cl, err := connect(nodeURL)
	if err != nil {
		return errors.Wrapf(err, "failed to connect to node at %q", nodeURL)
	}
	defer func() {
		if err := cl.Close(); err != nil {
			log.Fatalf("Failed to close client: %v", err)
		}
	}()

	st, err := internal.NewStorage(storagePath)
	if err != nil {
		return errors.Wrapf(err, "failed to open storage at %q", storagePath)
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Fatalf("Failed to close storage: %v", err)
		}
		log.Printf("Storage closed")
	}()

	h, err := st.Height(st.ReadOnlyTransaction())
	if err != nil {
		return errors.Wrap(err, "failed to get storage height")
	}
	if h == 0 {
		h = 1
	}
	for {
		select {
		case <-ctx.Done():
			log.Printf("User termination in progress...")
			return nil
		default:
			h, err = monitor(ctx, cl, st, h)
			if err != nil {
				if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
					continue
				}
				log.Printf("Restarting after error: %v (%T)", err, err)
				cl, err = connect(nodeURL)
				if err != nil {
					return errors.Wrapf(err, "failed to reconnect to node at %q", nodeURL)
				}
				continue
			}
		}
	}
}

func connect(node string) (*grpc.ClientConn, error) {
	return grpc.Dial(node,
		grpc.WithTransportCredentials(insecure.NewCredentials()),    // Connection is insecure
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(tenMB)), // Enable messages up to 10MB
	)
}

func monitor(ctx context.Context, conn *grpc.ClientConn, storage *internal.Storage, start uint32) (uint32, error) {
	log.Printf("Starting from height: %d", start)
	ph := start
	c := bu.NewBlockchainUpdatesApiClient(conn)
	req := &bu.SubscribeRequest{
		FromHeight: int32(start),
	}
	stream, err := c.Subscribe(ctx, req)
	if err != nil {
		return start, err
	}
	var event = new(bu.SubscribeEvent)
	for err = stream.RecvMsg(event); err == nil; err = stream.RecvMsg(event) {
		h, err := handleUpdate(storage, event)
		if err != nil {
			return ph, err
		}
		ph = h
	}
	return ph, err
}

func handleUpdate(storage *internal.Storage, event *bu.SubscribeEvent) (uint32, error) {
	update := event.GetUpdate()
	height := uint32(update.GetHeight())

	txn := storage.Transaction()
	defer txn.Discard()

	switch u := update.GetUpdate().(type) {
	case *events.BlockchainUpdated_Append_:
		if b := u.Append.GetBlock(); b != nil {
			aa, err := extractAccounts(height, b.GetBlock().GetTransactions(), u.Append.GetTransactionStateUpdates(), u.Append.GetTransactionsMetadata())
			if err != nil {
				return 0, err
			}
			accounts, transactions, groups, speedup := groupTransactions(aa)
			if err := storage.UpdateBlockInfo(txn, height, accounts, transactions, groups, speedup); err != nil {
				return 0, err
			}
			if len(aa) > 0 {
				log.Printf("\tHeight: %d\tAccounts: %d\tTransactions: %d\tGroups: %d\tSpeedup: %.2f",
					height, accounts, transactions, groups, speedup)
			}
		}
	case *events.BlockchainUpdated_Rollback_:
	default:
		return 0, errors.Errorf("unsupported event type %T at height %d", update.Update, update.Height)
	}
	if err := txn.Commit(); err != nil {
		return 0, errors.Wrap(err, "failed to commit changes")
	}
	return height, nil
}

func extractAccounts(h uint32, txs []*waves.SignedTransaction, updates []*events.StateUpdate, meta []*events.TransactionMetadata) ([][]proto.AddressID, error) {
	r := make([][]proto.AddressID, len(txs))
	for i, tx := range txs {
		switch tk := tx.Transaction.(type) {
		case *waves.SignedTransaction_WavesTransaction:
			switch tt := tk.WavesTransaction.Data.(type) {
			case *waves.Transaction_Genesis:
				id, err := publicKeyHashToID(tt.Genesis.GetRecipientAddress())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id)
			case *waves.Transaction_Payment:
				id1, err := publicKeyToID(tk.WavesTransaction.SenderPublicKey)
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id1)
				id2, err := publicKeyHashToID(tt.Payment.GetRecipientAddress())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id2)
			case *waves.Transaction_Issue:
			case *waves.Transaction_Transfer:
				m := meta[i]
				id1, err := wavesAddressToID(m.GetSenderAddress())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id1)
				id2, err := wavesAddressToID(m.GetTransfer().GetRecipientAddress())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id2)
			case *waves.Transaction_Reissue:
			case *waves.Transaction_Burn:
			case *waves.Transaction_Exchange:
				id1, err := publicKeyToID(tk.WavesTransaction.SenderPublicKey)
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id1)
				for j, o := range tt.Exchange.GetOrders() {
					id2, err := publicKeyToID(o.GetSenderPublicKey())
					if err != nil {
						return nil, errors.Wrapf(err, "failed order %d of transaction %d at height %d", j+1, i+1, h)
					}
					r[i] = append(r[i], id2)
				}
			case *waves.Transaction_Lease:
				id1, err := publicKeyToID(tk.WavesTransaction.SenderPublicKey)
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id1)
				id2, err := wavesAddressToID(meta[i].GetLease().GetRecipientAddress())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id2)
			case *waves.Transaction_LeaseCancel:
				id1, err := publicKeyToID(tk.WavesTransaction.SenderPublicKey)
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id1)
				for j, l := range updates[i].GetIndividualLeases() {
					id2, err := wavesAddressToID(l.GetRecipient())
					if err != nil {
						return nil, errors.Wrapf(err, "failed on leasing %d of transaction %d at height %d", j+1, i+1, h)
					}
					r[i] = append(r[i], id2)
				}
			case *waves.Transaction_CreateAlias:
			case *waves.Transaction_MassTransfer:
				id1, err := publicKeyToID(tk.WavesTransaction.SenderPublicKey)
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id1)
				for j, ra := range meta[i].GetMassTransfer().GetRecipientsAddresses() {
					id2, err := wavesAddressToID(ra)
					if err != nil {
						return nil, errors.Wrapf(err, "failed on recipient %d of transaction %d at height %d", j+1, i+1, h)
					}
					r[i] = append(r[i], id2)
				}
			case *waves.Transaction_DataTransaction:
			case *waves.Transaction_SetScript:
			case *waves.Transaction_SponsorFee:
			case *waves.Transaction_SetAssetScript:
			case *waves.Transaction_InvokeScript:
				m := meta[i]
				id1, err := wavesAddressToID(m.GetSenderAddress())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id1)
				id2, err := wavesAddressToID(m.GetInvokeScript().GetDAppAddress())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id2)
				acs, err := handleInvokeResult(meta[i].GetInvokeScript().GetResult())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], acs...)
			case *waves.Transaction_UpdateAssetInfo:
			case *waves.Transaction_InvokeExpression:
			default:
				return nil, errors.Errorf("unsupported transaction type '%T'", tx)
			}
		case *waves.SignedTransaction_EthereumTransaction:
			m := meta[i]
			id1, err := wavesAddressToID(m.GetSenderAddress())
			if err != nil {
				return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
			}
			r[i] = append(r[i], id1)
			switch at := m.GetEthereum().GetAction().(type) {
			case *events.TransactionMetadata_EthereumMetadata_Transfer:
				id2, err := wavesAddressToID(at.Transfer.GetRecipientAddress())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id2)
			case *events.TransactionMetadata_EthereumMetadata_Invoke:
				id2, err := wavesAddressToID(at.Invoke.GetDAppAddress())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], id2)
				acs, err := handleInvokeResult(at.Invoke.GetResult())
				if err != nil {
					return nil, errors.Wrapf(err, "failed on transaction %d at height %d", i+1, h)
				}
				r[i] = append(r[i], acs...)
			default:
				return nil, errors.Errorf("usupported transaction action type '%T'", m.GetEthereum().GetAction())
			}
		default:
			return nil, errors.Errorf("unsupported transaction kind '%T'", tx)
		}
	}
	return r, nil
}

func handleInvokeResult(res *waves.InvokeScriptResult) ([]proto.AddressID, error) {
	r := make([]proto.AddressID, 0)
	for i, tr := range res.GetTransfers() {
		id, err := publicKeyHashToID(tr.GetAddress())
		if err != nil {
			return nil, errors.Wrapf(err, "failed on transfer %d", i+1)
		}
		r = append(r, id)
	}
	for i, l := range res.GetLeases() {
		id, err := publicKeyHashToID(l.GetRecipient().GetPublicKeyHash())
		if err != nil {
			return nil, errors.Wrapf(err, "failed on leasing %d", i+1)
		}
		r = append(r, id)
	}
	for i, inv := range res.GetInvokes() {
		id, err := wavesAddressToID(inv.GetDApp())
		if err != nil {
			return nil, errors.Wrapf(err, "failed on invoke %d", i+1)
		}
		r = append(r, id)
		as, err := handleInvokeResult(inv.GetStateChanges())
		if err != nil {
			return nil, err
		}
		r = append(r, as...)
	}
	return r, nil
}

func groupTransactions(transactions [][]proto.AddressID) (uint32, uint32, uint32, float32) {
	am := map[proto.AddressID]struct{}{}
	m := map[int][]proto.AddressID{}
	index := 0
	for i := range transactions {
		addresses := transactions[i]
		for _, a := range addresses {
			am[a] = struct{}{}
		}
		var groups []int
		for k, v := range m {
			if intersects(v, addresses) {
				groups = append(groups, k)
			}
		}
		if len(groups) == 0 { // No intersections, add new group of addresses
			m[index] = deduplicate(addresses)
			index++
		} else { // Join intersected groups in one and add addresses to it
			sort.Ints(groups)
			gi := groups[0]
			for j := 1; j < len(groups); j++ {
				idx := groups[j]
				m[gi] = append(m[gi], m[idx]...)
				delete(m, idx)
			}
			m[gi] = deduplicate(append(m[gi], addresses...))
		}
	}
	tc := len(transactions)
	gc := len(m)
	p := float32(gc) / float32(tc)
	s := 1 / ((1 - p) + (p / float32(gc)))
	if math.IsNaN(float64(s)) || math.IsInf(float64(s), 0) {
		s = 0
	}
	return uint32(len(am)), uint32(tc), uint32(gc), s
}

func deduplicate(a []proto.AddressID) []proto.AddressID {
	r := make([]proto.AddressID, 0)
	for _, x := range a {
		if contains(r, x) {
			continue
		}
		r = append(r, x)
	}
	return r
}

func intersects(a, b []proto.AddressID) bool {
	for _, x := range a {
		if contains(b, x) {
			return true
		}
	}
	return false
}

func contains(a []proto.AddressID, x proto.AddressID) bool {
	for _, y := range a {
		if y == x {
			return true
		}
	}
	return false
}

func publicKeyToID(pk []byte) (proto.AddressID, error) {
	if len(pk) != crypto.PublicKeySize {
		return proto.AddressID{}, errors.Errorf("invalid public key size %d", len(pk))
	}
	h, err := crypto.SecureHash(pk[:])
	if err != nil {
		return proto.AddressID{}, err
	}
	var r proto.AddressID
	copy(r[:], h[:proto.AddressIDSize])
	return r, nil
}

func wavesAddressToID(addr []byte) (proto.AddressID, error) {
	if len(addr) != proto.WavesAddressSize {
		return proto.AddressID{}, errors.Errorf("invalid waves address size %d", len(addr))
	}
	var r proto.AddressID
	copy(r[:], addr[2:2+proto.AddressIDSize])
	return r, nil
}

func publicKeyHashToID(h []byte) (proto.AddressID, error) {
	if len(h) != proto.AddressIDSize {
		return proto.AddressID{}, errors.Errorf("invalid public key hash size %d", len(h))
	}
	var r proto.AddressID
	copy(r[:], h[:proto.AddressIDSize])
	return r, nil
}
