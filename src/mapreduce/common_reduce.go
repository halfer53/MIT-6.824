package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	debug("%v Reduce phase\n", outFile)
	inputfiles := make([]*os.File, nMap)
	decoders := make([]*json.Decoder, nMap)
	rmap := make(map[string][]string)
	outfile, err := os.OpenFile(outFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	defer outfile.Close()
	if err != nil {
		log.Fatalln("open", outfile, err)
		return
	}

	for m := 0; m < nMap; m++ {
		filename := reduceName(jobName, m, reduceTask)
		inputfiles[m], err = os.Open(filename)
		if err != nil {
			log.Fatalln("read", filename, err)
			return
		}
		defer inputfiles[m].Close()
		decoders[m] = json.NewDecoder(inputfiles[m])

		for {
			var keyval KeyValue
			if err := decoders[m].Decode(&keyval); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalln("decode", filename, err)
				return
			}

			debugVerbose("Reduce: %v, %v\n", keyval.Key, keyval.Value)

			_, ok := rmap[keyval.Key]
			if !ok {
				strlist := make([]string, 0)
				strlist = append(strlist, keyval.Value)
				rmap[keyval.Key] = strlist
			} else {
				rmap[keyval.Key] = append(rmap[keyval.Key], keyval.Value)
			}
		}

		var keys []string
		for k := range rmap {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		enc := json.NewEncoder(outfile)

		for _, key := range keys {
			values := rmap[key]
			//debug("Reduce input key:%v, value: %v, len %v\n", key, values, len(values))

			output := reduceF(key, values)
			enc.Encode(KeyValue{key, output})
		}

	}

}
