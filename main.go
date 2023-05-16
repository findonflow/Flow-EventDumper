package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/sanity-io/litter"
	"github.com/vmihailenco/msgpack"
)

func main() {

	//open badger database
	//https://github.com/onflow/flow-go/blob/cb148785274451cf005bed04c4fa27f2f557dab9/storage/badger/operation/prefix.go#L92
	db, err := badger.Open(badger.DefaultOptions("../../mainnet1").WithTruncate(true))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	stream := db.NewStream()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 32 // Set number of goroutines to use for iteration.
	//stream.Prefix = []byte{0x66} //tx        //events
	stream.Prefix = []byte{0x22} //tx        //transaction
	//stream.Prefix = []byte{0x68}        //txResult
	stream.LogPrefix = "Find.Streaming" // For identifying stream logs. Outputs to Logger.
	stream.KeyToList = nil

	count := 0
	stream.Send = func(list *pb.KVList) error {
		for _, kv := range list.GetKv() {
			//var err error
			count = count + 1
			if count%10000 == 0 {
				fmt.Println(count)
			}
			//k key

			k := kv.GetKey()
			//fmt.Println(len(k))
			fmt.Println(hex.EncodeToString(k))

			/*
				blockID := hex.EncodeToString(k[1:33])
				transactionID := hex.EncodeToString(k[33:65])
				transactionIndex := hex.EncodeToString(k[65:69])
				transactionIndexInt, err := strconv.ParseInt(transactionIndex, 16, 32)
				if err != nil {
					panic(err)

				}

				eventIndex := hex.EncodeToString(k[69:73])
				eventIndexInt, err := strconv.ParseInt(eventIndex, 16, 32)
				if err != nil {
					panic(err)

				}
				fmt.Println(blockID, transactionID, transactionIndexInt, eventIndexInt)
			*/
			//		os.Exit(0)
			//v value
			//
			v := kv.GetValue()
			var event interface{}
			err = msgpack.Unmarshal(v, &event)
			if err != nil {
				return fmt.Errorf("could not decode the event: %w", err)
			}
			litter.Dump(event)
			os.Exit(0)
		}

		return nil
	}

	if err := stream.Orchestrate(context.Background()); err != nil {
		fmt.Println(err)
	}

	fmt.Scanln()
	for {

		time.Sleep(time.Second)
	}

}
