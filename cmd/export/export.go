package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/alexeykiselev/waves-tx-graph/internal"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
)

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
		storagePath string
	)
	flag.StringVar(&storagePath, "storage", "", "Path to application's storage")
	flag.Parse()

	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()

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
	accountsData := make([]uint32, 0, 1000)
	transactionsData := make([]uint32, 0, 1000)
	groupsData := make([]uint32, 0, 1000)
	pData := make([]float64, 0, 1000)
	for i := 1; i < int(h); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		accounts, transactions, groups, p, err := st.BlockInfo(st.ReadOnlyTransaction(), uint32(i))
		if err != nil {
			return err
		}
		if p > 0 {
			pData = append(pData, float64(p))
		}
		accountsData = append(accountsData, accounts)
		transactionsData = append(transactionsData, transactions)
		groupsData = append(groupsData, groups)
		if i%1000 == 0 {
			ad := stats.LoadRawData(accountsData)
			td := stats.LoadRawData(transactionsData)
			gd := stats.LoadRawData(groupsData)
			pd := stats.LoadRawData(pData)
			aMean, err := ad.Mean()
			if err != nil {
				return err
			}
			tMean, err := td.Mean()
			if err != nil {
				return err
			}
			gMean, err := gd.Mean()
			if err != nil {
				return err
			}
			pMean, err := pd.Mean()
			if err != nil {
				return err
			}
			fmt.Printf("%d\t%.2f\t%.2f\t%.2f\t%.2f\n", i, aMean, tMean, gMean, pMean)
			accountsData = make([]uint32, 0, 1000)
			transactionsData = make([]uint32, 0, 1000)
			groupsData = make([]uint32, 0, 1000)
			pData = make([]float64, 0, 1000)
		}
	}
	return nil
}
