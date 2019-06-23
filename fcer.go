package fcer

import (
	"bufio"
	"io"
	"os"
	"time"
)

// Partition holds informations about a single part of a file. This part is
// described by providing an absolute offset (from file-beginning 0) and a
// length variable.
//
// The intention is, that an io.Reader starts to read at Offset and reads
// Length bytes afterwards. If every worker has exactly one partition and
// acts against this rules there can be multiple workers processing a single
// file simultaneously without interference.
type Partition struct {
	Offset int64
	Length int64
}

// FindPartitions find a partition-distribution for the passed file with
// approximatly `intededWorkers` partition splits. If the suggested workers
// variable is to high for the input file, fewer partitions will be returned.
func FindPartitions(file *os.File, intendedWorkers int) ([]*Partition, error) {
	defer func(start time.Time) {
		Log.Info("finding partitions took %s", time.Now().Sub(start).String())
	}(time.Now())

	stats, err := file.Stat()
	if err != nil {
		return nil, err
	}

	totalSize := stats.Size()
	Log.Info("total file size: %d", totalSize)

	partitionSize := totalSize / int64(intendedWorkers)
	Log.Info("size for each worker aproximatly: %d", partitionSize)

	return findPartitions(file, totalSize, partitionSize, intendedWorkers)
}

func findPartitions(file *os.File, totalSize, partitionSize int64, intendedWorkers int) ([]*Partition, error) {
	partitions := make([]*Partition, intendedWorkers)

	currentStart := int64(0)
	currentEnd := int64(partitionSize)

	for idx := 0; idx < intendedWorkers; idx++ {
		// last partition is smaller, because it needs to compensate for all
		// the "line-overflows" from all previous parts
		if idx == intendedWorkers-1 {
			partitions[idx] = &Partition{
				Offset: currentStart,
				Length: totalSize - currentStart,
			}
		} else {
			// read temp buffer at offset. It is used to find the correct
			// offset for partition-splitting, in order to keep lines in a
			// single partition
			tmpBuf := make([]byte, 50)

			_, err := file.ReadAt(tmpBuf, currentEnd)
			if err != nil {
				// if we read into an EOF the worker size is too big. In order
				// to continue execution anyway use this part as the ending
				// and reduce amount of workers afterwards
				if err == io.EOF {
					partitions[idx] = &Partition{
						Offset: currentStart,
						Length: totalSize - currentStart,
					}
					partitions = partitions[0 : idx+1]
					break
				}

				return nil, err
			}

			Log.Debug("searching for partition=%d. start=%d end=%d. Current tmpBuf is %q", idx, currentStart, currentEnd, string(tmpBuf))

			// find next splitting new line for this partion
			for skip := 0; skip < len(tmpBuf); skip++ {
				if tmpBuf[skip] == '\n' {
					// add one because loop-incrementer is index and we need an absolute value
					skip++

					Log.Debug("adding skip=%d for partition=%d", skip, idx)
					currentEnd += int64(skip)

					partitions[idx] = &Partition{
						Offset: currentStart,
						Length: currentEnd - currentStart,
					}

					break
				}
			}
		}

		currentStart = currentEnd
		currentEnd += partitionSize
	}

	return partitions, nil
}

// PrintPartitionInfo prints the information associated with each partition.
// Furthermore it also displays the first line of every partition. This is
// useful if you want to know where exactly the data was split. Printing is
// fast and no full file-iteration is needed. The printer seeks to the offset
// points using the meta-information of the partitions.
func PrintPartitionInfo(file *os.File, partitions []*Partition) error {
	for idx, p := range partitions {
		Log.Info("Partition %d starts at offset %d and reads %d bytes", idx, p.Offset, p.Length)

		_, err := file.Seek(p.Offset, io.SeekStart)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(file)
		scanner.Scan()

		Log.Info("First line of partition=%d is %q", idx, scanner.Text())
	}

	return nil
}
