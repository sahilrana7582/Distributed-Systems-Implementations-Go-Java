package main

import (
	"fmt"
	"log"

	"github.com/sahilrana7582/raftlog/internal/wal"
)

func main() {
	w, err := wal.New("./data/node1")
	if err != nil {
		log.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	fmt.Println("last index:", w.LastIndex())

	// append entries 1..3 (example). Make sure to pick correct index: if WAL already has entries,
	// pick last+1.
	next := w.LastIndex() + 1
	for i := uint64(0); i < 3; i++ {
		e := wal.Entry{
			Index: next + i,
			Term:  1,
			Type:  wal.EntryTypeNormal,
			Data:  []byte(fmt.Sprintf("cmd-%d", next+i)),
		}
		li, err := w.Append(e)
		if err != nil {
			log.Fatalf("append: %v", err)
		}
		fmt.Println("appended, new last index:", li)
	}

	// read them back
	for idx := w.LastIndex() - 2; idx <= w.LastIndex(); idx++ {
		ent, err := w.ReadAt(idx)
		if err != nil {
			log.Fatalf("readAt %d: %v", idx, err)
		}
		fmt.Printf("entry %d term=%d data=%s\n", ent.Index, ent.Term, string(ent.Data))
	}
}
